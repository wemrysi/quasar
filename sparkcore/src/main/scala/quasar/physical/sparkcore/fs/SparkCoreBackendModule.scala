/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar._
import quasar.connector.BackendModule
import quasar.contrib.pathy._
import quasar.common._
import quasar.fp._, free._
import quasar.fs._, FileSystemError._
import quasar.fs.mount._, BackendDef._
import quasar.effect._
import quasar.qscript.{Read => _, _}

import scala.Predef.implicitly

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import matryoshka._
import matryoshka.implicits._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

trait SparkCoreBackendModule extends BackendModule {

  // conntector specificc
  type Eff[A]
  def toLowerLevel[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): M ~> Free[S, ?]
  def generateSC: Config => DefErrT[M, SparkContext]

  def ReadSparkContextInj: Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj: Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj: Inject[MonotonicSeq, Eff]
  def TaskInj: Inject[Task, Eff]
  def SparkConnectorDetailsInj: Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, queryfile.RddState, ?], Eff]

  // common for all spark based connecotrs

  type M[A] = Free[Eff, A]
  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr = RDD[Data]

  private final implicit def _ReadSparkContextInj: Inject[Read[SparkContext, ?], Eff] =
    ReadSparkContextInj
  private final implicit def _RFKeyValueStoreInj: Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff] =
    RFKeyValueStoreInj
  private final implicit def _MonotonicSeqInj: Inject[MonotonicSeq, Eff] =
    MonotonicSeqInj
  private final implicit def _TaskInj: Inject[Task, Eff] =
    TaskInj
  private final implicit def _SparkConnectorDetailsInj: Inject[SparkConnectorDetails, Eff] =
    SparkConnectorDetailsInj
  private final implicit def _QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, queryfile.RddState, ?], Eff] =
    QFKeyValueStoreInj

  def detailsOps: SparkConnectorDetails.Ops[Eff] = SparkConnectorDetails.Ops[Eff]
  def readScOps: Read.Ops[SparkContext, Eff] = Read.Ops[SparkContext, Eff]
  def msOps: MonotonicSeq.Ops[Eff] = MonotonicSeq.Ops[Eff]
  def qfKvsOps: KeyValueStore.Ops[QueryFile.ResultHandle, queryfile.RddState, Eff] =
    KeyValueStore.Ops[QueryFile.ResultHandle, queryfile.RddState, Eff]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type LowerLevel[A] = Coproduct[Task, PhysErr, A]
  def lowerToTask: LowerLevel ~> Task = λ[LowerLevel ~> Task](_.fold(
    injectNT[Task, Task],
    Failure.mapError[PhysicalError, Exception](_.cause) andThen Failure.toCatchable[Task, Exception]
  ))
  def toTask: M ~> Task = toLowerLevel[LowerLevel] andThen foldMapNT(lowerToTask)

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] =
    EitherT(toTask(generateSC(cfg).run)).map(sc => (toTask, Task.delay(sc.stop())))

  def rddFrom: AFile => M[RDD[Data]] = (f: AFile) => detailsOps.rddFrom(f)
  def first: RDD[Data] => M[Data] = (rdd: RDD[Data]) => lift(Task.delay {rdd.first}).into[Eff]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): Backend[Repr] =
    includeError((readScOps.ask >>= { sc =>
      val total = implicitly[Planner[QSM[T, ?], Eff]]
      cp.cataM(total.plan(rddFrom, first)).eval(sc).run.map(_.leftMap(pe => qscriptPlanningFailed(pe)))
    }).liftB)

  object SparkReadFileModule extends ReadFileModule {
    import ReadFile._
    import quasar.fp.numeric.{Natural, Positive}

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] =
      includeError(readfile.open[Eff](file, offset, limit).liftB)
    def read(h: ReadHandle): Backend[Vector[Data]] =
      includeError(readfile.read[Eff](h).liftB)
    def close(h: ReadHandle): Configured[Unit] =
      readfile.close[Eff](h).liftM[ConfiguredT]
  }
  def ReadFileModule = SparkReadFileModule

  object SparkQueryFileModule extends QueryFileModule {
    import QueryFile._
    import queryfile.RddState

    def executePlan(rdd: RDD[Data], out: AFile): Backend[AFile] = {
      val execute = detailsOps.storeData(rdd, out).as(out)
      val log     = PhaseResult.detail("RDD", rdd.toDebugString)
      withLog[AFile](execute, log)
    }

    def evaluatePlan(rdd: Repr): Backend[ResultHandle] = withLog(for {
      h <- msOps.next.map(ResultHandle(_))
      _ <- qfKvsOps.put(h, RddState(rdd.zipWithIndex.persist.some, 0))
    } yield h, PhaseResult.detail("RDD", rdd.toDebugString))

    def more(h: ResultHandle): Backend[Vector[Data]] = includeError(queryfile.more[Eff](h).liftB)

    def close(h: ResultHandle): Configured[Unit] = queryfile.close[Eff](h).liftM[ConfiguredT]

    def explain(rdd: RDD[Data]): Backend[String] =
      rdd.toDebugString.point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      includeError(detailsOps.listContents(dir).run.liftB)

    def fileExists(file: AFile): Configured[Boolean] =
      detailsOps.fileExists(file).liftM[ConfiguredT]
  }

  def QueryFileModule: QueryFileModule = SparkQueryFileModule

  // utility functions
  def includeError[A](b: Backend[FileSystemError \/ A]): Backend[A] = EitherT(b.run.map(_.join))
  def withLog[A](m: M[A], pr: PhaseResult): Backend[A] =
    WriterT(m.liftM[ConfiguredT].map((Vector(pr), _))).liftM[FileSystemErrT]
}
