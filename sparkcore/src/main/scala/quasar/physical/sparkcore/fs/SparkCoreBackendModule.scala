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
import quasar.fp._, free._
import quasar.fs._, FileSystemError._
import quasar.fs.mount._, BackendDef.DefErrT
import quasar.effect.Failure
import quasar.qscript._

import scala.Predef.implicitly

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import matryoshka._
import matryoshka.implicits._
import scalaz.{Failure => _, _}
import scalaz.concurrent.Task

trait SparkCoreBackendModule extends BackendModule {

  // conntector specificc
  type Eff[A]
  def toLowerLevel[S[_]](implicit S0: Task :<: S, S1: PhysErr :<: S): M ~> Free[S, ?]
  def generateSC: Config => DefErrT[M, SparkContext]
  def askSc: M[SparkContext]

  // common for all spark based connecotrs
  type M[A] = Free[Eff, A]
  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr = RDD[Data]

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

  def rddFrom: AFile => M[RDD[Data]]
  def first: RDD[Data] => M[Data]
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): Backend[Repr] =
    includeError((askSc >>= { sc =>
      val total = implicitly[Planner[QSM[T, ?], Eff]]
      cp.cataM(total.plan(rddFrom, first)).eval(sc).run.map(_.leftMap(pe => qscriptPlanningFailed(pe)))
    }).liftB)

  // util functions because 4 levels of monad transformes is like o_O
  // this will disapper
  def includeError[A](b: Backend[FileSystemError \/ A]): Backend[A]
}
