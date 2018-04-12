/*
 * Copyright 2014–2018 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.mongodb

import slamdata.Predef._
import quasar._, Planner._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT, PhaseResultTell}
import quasar.connector.BackendModule
import quasar.contrib.pathy.{ADir, AFile}
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.{FileSystemError, MonadFsErr}, FileSystemError.qscriptPlanningFailed
import quasar.physical.mongodb.WorkflowBuilder._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.{selector => _, _}
import quasar.physical.mongodb.planner.common._
import quasar.physical.mongodb.workflow._
import quasar.qscript._, RenderQScriptDSL._
import quasar.qscript.rewrites.{Coalesce => _, Optimize, PreferProjection, Rewrite}

import java.time.Instant
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import org.bson.BsonDocument
import scalaz._, Scalaz._

object MongoDbPlanner {

  trait Planner[F[_]] {
    type IT[G[_]]

    def plan
      [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
      (cfg: PlannerConfig[IT, EX, WF, M])
      (implicit
        ev0: WorkflowOpCoreF :<: WF,
        ev1: RenderTree[WorkflowBuilder[WF]],
        ev2: WorkflowBuilder.Ops[WF],
        ev3: ExprOpCoreF :<: EX,
        ev4: EX :<: ExprOp):
        AlgebraM[M, F, WorkflowBuilder[WF]]
  }

  object Planner {
    type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }

    def apply[T[_[_]], F[_]](implicit ev: Planner.Aux[T, F]) = ev

    implicit def shiftedReadFile[T[_[_]]: BirecursiveT: ShowT]
        : Planner.Aux[T, Const[ShiftedRead[AFile], ?]] =
      new ShiftedReadPlanner[T]

    implicit def qscriptCore[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, QScriptCore[T, ?]] =
      new QScriptCorePlanner[T]

    implicit def equiJoin[T[_[_]]: BirecursiveT: EqualT: ShowT]:
        Planner.Aux[T, EquiJoin[T, ?]] =
      new EquiJoinPlanner[T]

    implicit def coproduct[T[_[_]], F[_], G[_]](
      implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
        Planner.Aux[T, Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        type IT[G[_]] = T[G]
        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF, M])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: ExprOpCoreF :<: EX,
            ev4: EX :<: ExprOp) =
          _.run.fold(
            F.plan[M, WF, EX](cfg),
            G.plan[M, WF, EX](cfg))
      }

    // TODO: All instances below here only need to exist because of `FreeQS`,
    //       but can’t actually be called.

    def default[T[_[_]], F[_]](label: String): Planner.Aux[T, F] =
      new Planner[F] {
        type IT[G[_]] = T[G]

        def plan
          [M[_]: Monad: ExecTimeR: MonadFsErr, WF[_]: Functor: Coalesce: Crush, EX[_]: Traverse]
          (cfg: PlannerConfig[T, EX, WF, M])
          (implicit
            ev0: WorkflowOpCoreF :<: WF,
            ev1: RenderTree[WorkflowBuilder[WF]],
            ev2: WorkflowBuilder.Ops[WF],
            ev3: ExprOpCoreF :<: EX,
            ev4: EX :<: ExprOp) =
          κ(raiseErr(qscriptPlanningFailed(InternalError.fromMsg(s"should not be reached: $label"))))
      }

    implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
      default("DeadEnd")

    implicit def read[T[_[_]], A]: Planner.Aux[T, Const[Read[A], ?]] =
      default("Read")

    implicit def shiftedReadDir[T[_[_]]]: Planner.Aux[T, Const[ShiftedRead[ADir], ?]] =
      default("ShiftedRead[ADir]")

    implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
      default("ThetaJoin")

    implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
      default("ProjectBucket")
  }

  // TODO: This should perhaps be _in_ PhaseResults or something
  def log[M[_]: Monad, A: RenderTree]
    (label: String, ma: M[A])
    (implicit mtell: MonadTell_[M, PhaseResults])
      : M[A] =
    ma.mproduct(a => mtell.tell(Vector(PhaseResult.tree(label, a)))) ∘ (_._1)

  def toMongoQScript[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: MonadFsErr: PhaseResultTell]
      (anyDoc: Collection => OptionT[M, BsonDocument],
        qs: T[fs.MongoQScript[T, ?]])
      (implicit BR: Branches[T, fs.MongoQScript[T, ?]])
      : M[T[fs.MongoQScript[T, ?]]] = {

    type MQS[A] = fs.MongoQScript[T, A]
    type QST[A] = QScriptTotal[T, A]

    val O = new Optimize[T]
    val R = new Rewrite[T]

    def normalize(mqs: T[MQS]): M[T[MQS]] = {
      val mqs1 = mqs.transCata[T[MQS]](R.normalizeEJ[MQS])
      val mqs2 = BR.branches.modify(
          _.transCata[FreeQS[T]](liftCo(R.normalizeEJCoEnv[QScriptTotal[T, ?]]))
        )(mqs1.project).embed
      Trans(assumeReadType[T, MQS, M](Type.AnyObject), mqs2)
    }

    // TODO: All of these need to be applied through branches. We may also be able to compose
    //       them with normalization as the last step and run until fixpoint. Currently plans are
    //       too sensitive to the order in which these are applied.
    //       Some constraints:
    //       - elideQuasarSigil should only be applied once
    //       - elideQuasarSigil needs assumeReadType to be applied in order to
    //         work properly in all cases
    //       - R.normalizeEJ/R.normalizeEJCoEnv may change the structure such
    //         that assumeReadType can elide more guards
    //         E.g. Map(x, SrcHole) is normalized into x. assumeReadType does
    //         not recognize any Map as shape preserving, but it may recognize
    //         x being shape preserving (e.g. when x = ShiftedRead(y, ExcludeId))
    for {
      mongoQS1 <- Trans(assumeReadType[T, MQS, M](Type.AnyObject), qs)
      mongoQS2 <- mongoQS1.transCataM(elideQuasarSigil[T, MQS, M](anyDoc))
      mongoQS3 <- normalize(mongoQS2)
      _ <- BackendModule.logPhase[M](PhaseResult.treeAndCode("QScript Mongo", mongoQS3))

      mongoQS4 =  mongoQS3.transCata[T[MQS]](
                    liftFF[QScriptCore[T, ?], MQS, T[MQS]](
                      repeatedly(O.subsetBeforeMap[MQS, MQS](
                        reflNT[MQS]))))
      _ <- BackendModule.logPhase[M](
             PhaseResult.treeAndCode("QScript Mongo (Subset Before Map)",
             mongoQS4))

      // TODO: Once field deletion is implemented for 3.4, this could be selectively applied, if necessary.
      mongoQS5 =  PreferProjection.preferProjection[MQS](mongoQS4)
      _ <- BackendModule.logPhase[M](PhaseResult.treeAndCode("QScript Mongo (Prefer Projection)", mongoQS5))
    } yield mongoQS5
  }

  def buildWorkflow
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR,
      WF[_]: Functor: Coalesce: Crush,
      EX[_]: Traverse]
    (cfg: PlannerConfig[T, EX, WF, M])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: ExprOpCoreF :<: EX,
      ev2: EX :<: ExprOp,
      ev3: RenderTree[Fix[WF]])
      : M[Fix[WF]] =
    for {
      wb <- log(
        "Workflow Builder",
        qs.cataM[M, WorkflowBuilder[WF]](
          Planner[T, fs.MongoQScript[T, ?]].plan[M, WF, EX](cfg).apply(_) ∘
            (_.transCata[Fix[WorkflowBuilderF[WF, ?]]](repeatedly(WorkflowBuilder.normalize[WF, Fix[WorkflowBuilderF[WF, ?]]])))))
      wf <- log("Workflow (raw)", liftM[M, Fix[WF]](WorkflowBuilder.build[WBM, WF](wb, cfg.queryModel)))
    } yield wf

  def plan0
    [T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      M[_]: Monad: ExecTimeR,
      WF[_]: Traverse: Coalesce: Crush: Crystallize,
      EX[_]: Traverse]
    (anyDoc: Collection => OptionT[M, BsonDocument],
      cfg: PlannerConfig[T, EX, WF, M])
    (qs: T[fs.MongoQScript[T, ?]])
    (implicit
      ev0: WorkflowOpCoreF :<: WF,
      ev1: WorkflowBuilder.Ops[WF],
      ev2: ExprOpCoreF :<: EX,
      ev3: EX :<: ExprOp,
      ev4: RenderTree[Fix[WF]],
      ME:  MonadFsErr[M],
      MT:  PhaseResultTell[M])
      : M[Crystallized[WF]] = {

    def doBuildWorkflow[F[_]: Monad: ExecTimeR]
      (cfg0: PlannerConfig[T, EX, WF, F])
      (qs0: T[fs.MongoQScript[T, ?]])
      (implicit ME: MonadFsErr[F])
        : F[FileSystemError \/ (PhaseResults, Fix[WF])] = {
      val fh = cfg0.funcHandler andThen (_.liftM[PhaseResultT])
      // NB: buildWorkflow[T, FileSystemErrT[PhaseResultT[F, ?], ?], WF, EX]
      // gives the right return type F[(PhaseResults, FileSystemError \/ Fix[WF])]
      // but adding a second FileSystemErrT screws up error handling:
      // unimplemented MapFunc's in FuncHandler don't fall back to JsFuncHandler
      // anymore
      ME.attempt(buildWorkflow[T, PhaseResultT[F, ?], WF, EX](
        cfg0.copy(funcHandler = fh))(qs0).run)
    }

    for {
      qs0 <- toMongoQScript[T, M](anyDoc, qs)
      res0 <- doBuildWorkflow[M](cfg)(qs0)
      wf0 <- res0 match {
               case \/-((_, wf)) if (needsMapBeforeSort(wf)) =>
                 // TODO look into adding mapBeforeSort to WorkflowBuilder or Workflow stage
                 // instead, so that we can avoid having to rerun some transformations.
                 // See #3063
                 log("QScript Mongo (Map Before Sort)",
                   Trans(mapBeforeSort[T, M], qs0)) >>= buildWorkflow[T, M, WF, EX](cfg)
               case \/-((log, wf)) =>
                 MT.tell(log) *> wf.point[M]
               case -\/(err) =>
                 raiseErr[M, Fix[WF]](err)
             }
      wf1 <- log(
        "Workflow (crystallized)",
        Crystallize[WF].crystallize(wf0).point[M])
    } yield wf1
  }

  def planExecTime[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument],
      execTime: Instant)
      : M[Crystallized[WorkflowF]] = {
    val peek = anyDoc andThen (_.mapT(_.liftM[ReaderT[?[_], Instant, ?]]))
    plan[T, ReaderT[M, Instant, ?]](qs, queryContext, queryModel, peek).run(execTime)
  }

  /** Translate the QScript plan to an executable MongoDB "physical"
    * plan, taking into account the current runtime environment as captured by
    * the given context.
    *
    * Internally, the type of the plan being built constrains which operators
    * can be used, but the resulting plan uses the largest, common type so that
    * callers don't need to worry about it.
    *
    * @param anyDoc returns any document in the given `Collection`
    */
  def plan[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: PhaseResultTell: MonadFsErr: ExecTimeR](
      qs: T[fs.MongoQScript[T, ?]],
      queryContext: fs.QueryContext,
      queryModel: MongoQueryModel,
      anyDoc: Collection => OptionT[M, BsonDocument])
      : M[Crystallized[WorkflowF]] = {
    import MongoQueryModel._

    val bsonVersion = toBsonVersion(queryModel)

    def joinHandler[WF[_]: Functor: Coalesce: Crush: Crystallize]
      (implicit ev0: Classify[WF], ev1: WorkflowOpCoreF :<: WF, ev2: RenderTree[WorkflowBuilder[WF]])
        : JoinHandler[WF, WBM] =
      JoinHandler.fallback[WF, WBM](
        JoinHandler.pipeline[WBM, WF](queryModel, queryContext.statistics, queryContext.indexes),
        JoinHandler.mapReduce[WBM, WF](queryModel))

    queryModel match {
      case `3.4.4` =>
        val cfg = PlannerConfig[T, Expr3_4_4, Workflow3_4F, M](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4_4[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4_4](anyDoc, cfg)(qs)

      case `3.4` =>
        val cfg = PlannerConfig[T, Expr3_4, Workflow3_4F, M](
          joinHandler[Workflow3_4F],
          FuncHandler.handle3_4[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_4F, Expr3_4](anyDoc, cfg)(qs)

      case `3.2` =>
        val cfg = PlannerConfig[T, Expr3_2, Workflow3_2F, M](
          joinHandler[Workflow3_2F],
          FuncHandler.handle3_2[MapFunc[T, ?], M](bsonVersion),
          StaticHandler.handle,
          queryModel,
          bsonVersion)
        plan0[T, M, Workflow3_2F, Expr3_2](anyDoc, cfg)(qs).map(_.inject[WorkflowF])

    }
  }
}
