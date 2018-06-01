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
import quasar.contrib.cats.effect._
import quasar.contrib.fs2.convert
import quasar.contrib.pathy._
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

import cats.effect.{IO, LiftIO}
import fs2.{Chunk, Stream}
import fs2.interop.scalaz._
import io.chrisdavenport.scalaz.task._
import matryoshka._
import matryoshka.implicits._
import scalaz._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.concurrent.Task
import shims._

final class MimirQScriptEvaluator[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: LiftIO: Monad: PlannerErrorME: MonadFinalizers[?[_], IO]] private (
    cake: Cake)
    extends QScriptEvaluator[T, AssociatesT[T, F, IO, ?], Stream[IO, Data]] {

  type MT[X[_], A] = Kleisli[X, Associates[T, F, IO], A]
  type M[A] = MT[F, A]

  type QS[U[_[_]]] = mimir.MimirQScriptCP[U]

  type Repr = mimir.MimirRepr

  implicit def QSMFromQScriptCoreI: Injectable.Aux[QScriptCore[T, ?], QSM] =
    Injectable.inject[QScriptCore[T, ?], QSM]

  implicit def QSMFromEquiJoinI: Injectable.Aux[EquiJoin[T, ?], QSM] =
    Injectable.inject[EquiJoin[T, ?], QSM]

  implicit def QSMFromShiftedReadI: Injectable.Aux[Const[ShiftedRead[AFile], ?], QSM] =
    Injectable.inject[Const[ShiftedRead[AFile], ?], QSM]

  implicit def QSMToQScriptTotal: Injectable.Aux[QSM, QScriptTotal[T, ?]] =
    mimir.qScriptToQScriptTotal[T]

  def QSMFunctor: Functor[QSM] =
    Functor[QSM]

  def QSMFromQScriptCore: QScriptCore[T, ?] :<: QSM =
    Inject[QScriptCore[T, ?], QSM]

  def UnirewriteT: Unirewrite[T, QS[T]] =
    implicitly[Unirewrite[T, QS[T]]]

  def UnicoalesceCap: Unicoalesce.Capture[T, QS[T]] =
    Unicoalesce.Capture[T, QS[T]]

  def execute(repr: Repr): M[ReadError \/ Stream[IO, Data]] =
    MimirQScriptEvaluator.slicesToStream(repr.table.slices)
      .filter(_ != JUndefined)
      .map(JValue.toData)
      .right[ReadError]
      .point[M]

  def optimize: QSM[T[QSM]] => QSM[T[QSM]] =
    (new Optimize[T]).optimize(reflNT[QSM])

  def plan(cp: T[QSM]): M[Repr] = {
    def qScriptCorePlanner =
      new mimir.QScriptCorePlanner[T, M](
        λ[ReaderT[Task, Cake, ?] ~> M](_.run(cake).to[IO].to[F].liftM[MT]))

    def equiJoinPlanner =
      new mimir.EquiJoinPlanner[T, M](λ[IO ~> M](_.to[F].liftM[MT]))

    def shiftedReadPlanner =
      new FederatedShiftedReadPlanner[T, F](cake)

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
      F[_]: LiftIO: Monad: PlannerErrorME: MonadFinalizers[?[_], IO]](
      cake: Cake)
      : QScriptEvaluator[T, AssociatesT[T, F, IO, ?], Stream[IO, Data]] =
    new MimirQScriptEvaluator[T, F](cake)

  def slicesToStream[F[_]: Functor](slices: StreamT[F, Slice]): Stream[F, JValue] =
    convert.fromChunkedStreamT(slices.map(s => Chunk.indexedSeq(SliceIndexedSeq(s))))
}
