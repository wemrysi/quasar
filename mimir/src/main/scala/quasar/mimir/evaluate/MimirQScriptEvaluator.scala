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

package quasar.mimir.evaluate

import slamdata.Predef._

import quasar._
import quasar.api.ResourceError.ReadError
import quasar.blueeyes.json.{JValue, JUndefined}
import quasar.connector.QScriptEvaluator
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, readerT._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs.Planner.PlannerErrorME
import quasar.mimir
import quasar.mimir.MimirCake._
import quasar.qscript._
import quasar.qscript.rewrites.{Optimize, Unicoalesce, Unirewrite}
import quasar.yggdrasil.table.Slice

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import delorean._
import fs2.{Chunk, Stream}
import fs2.interop.scalaz._
import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.std.scalaFuture._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.concurrent.Task

final class MimirQScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: PlannerErrorME] private (
    cake: Cake,
    liftTask: Task ~> F)
    extends QScriptEvaluator[T, AssociatesT[T, F, Task, ?], Stream[Task, Data]] {

  type MT[X[_], A] = Kleisli[X, Associates[T, F, Task], A]
  type M[A] = MT[F, A]

  type QS[U[_[_]]] = mimir.MimirQScriptCP[U]

  type Repr = mimir.MimirRepr

  val taskToM: Task ~> M =
    liftMT[F, MT] compose liftTask

  def QSMFunctor: Functor[QSM] =
    Functor[QSM]

  implicit def QSMFromQScriptCore: QScriptCore[T, ?] :<: QSM =
    Inject[QScriptCore[T, ?], QSM]

  implicit def QSMFromEquiJoin: EquiJoin[T, ?] :<: QSM =
    Inject[EquiJoin[T, ?], QSM]

  implicit def QSMFromShiftedRead: Const[ShiftedRead[AFile], ?] :<: QSM =
    Inject[Const[ShiftedRead[AFile], ?], QSM]

  implicit def QSMToQScriptTotal: Injectable.Aux[QSM, QScriptTotal[T, ?]] =
    mimir.qScriptToQScriptTotal[T]

  def UnirewriteT: Unirewrite[T, QS[T]] =
    implicitly[Unirewrite[T, QS[T]]]

  def UnicoalesceCap: Unicoalesce.Capture[T, QS[T]] =
    Unicoalesce.Capture[T, QS[T]]

  def execute(repr: Repr): M[ReadError \/ Stream[Task, Data]] = {
    val slices =
      repr.table.slices.trans(λ[Future ~> Task](_.toTask))

    MimirQScriptEvaluator.slicesToStream(slices)
      .filter(_ != JUndefined)
      .map(JValue.toData)
      .right[ReadError]
      .point[M]
  }

  def optimize: QSM[T[QSM]] => QSM[T[QSM]] =
    (new Optimize[T]).optimize(reflNT[QSM])

  def plan(cp: T[QSM]): M[Repr] = {
    def qScriptCorePlanner =
      new mimir.QScriptCorePlanner[T, M](taskToM compose runReaderNT[Task, Cake](cake))

    def equiJoinPlanner =
      new mimir.EquiJoinPlanner[T, M](taskToM)

    def shiftedReadPlanner =
      new FederatedShiftedReadPlanner[T, F](cake, liftTask)

    lazy val planQST: AlgebraM[M, QScriptTotal[T, ?], Repr] =
      _.run.fold(
        qScriptCorePlanner.plan(planQST),
        _.run.fold(
          _ => ???,   // ProjectBucket
          _.run.fold(
            _ => ???,   // ThetaJoin
            _.run.fold(
              equiJoinPlanner.plan(planQST),
              _.run.fold(
                _ => ???,    // ShiftedRead[ADir]
                _.run.fold(
                  shiftedReadPlanner.plan,
                  _.run.fold(
                    _ => ???,   // Read[ADir]
                    _.run.fold(
                      _ => ???,   // Read[AFile]
                      _ => ???))))))))    // DeadEnd

    def planQSM(in: QSM[Repr]): M[Repr] =
      in.run.fold(
        qScriptCorePlanner.plan(planQST),
        _.run.fold(
          equiJoinPlanner.plan(planQST),
	        shiftedReadPlanner.plan))

    cp.cataM[M, Repr](planQSM _)
  }
}

object MimirQScriptEvaluator {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: Monad: PlannerErrorME](
      cake: Cake,
      liftTask: Task ~> F)
      : QScriptEvaluator[T, AssociatesT[T, F, Task, ?], Stream[Task, Data]] =
    new MimirQScriptEvaluator[T, F](cake, liftTask)

  def slicesToStream[F[_]: Functor](slices: StreamT[F, Slice]): Stream[F, JValue] =
    Stream.unfoldChunkEval(slices)(_.step.map(_(
      yieldd = (slice, st) => Some((Chunk.indexedSeq(SliceIndexedSeq(slice)), st))
    , skip = st => Some((Chunk.empty, st))
    , done = None)))
}
