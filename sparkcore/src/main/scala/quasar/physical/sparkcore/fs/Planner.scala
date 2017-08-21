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
import quasar._, quasar.Planner._
import quasar.contrib.pathy.{AFile, ADir}
import quasar.qscript._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz._, Scalaz._


trait Planner[F[_], M[_]] extends Serializable {
  def plan(fromFile: AFile => M[RDD[Data]], first: RDD[Data] => M[Data]): AlgebraM[Planner.SparkState[M, ?], F, RDD[Data]]
}

object Planner {

  def apply[F[_],M[_]:Monad](implicit P: Planner[F, M]): Planner[F, M] = P

  type SparkState[M[_], A] = StateT[EitherT[M, PlannerError, ?], SparkContext, A]
  type SparkStateT[F[_], A] = StateT[F, SparkContext, A]

  implicit def deadEnd[M[_]:Monad]: Planner[Const[DeadEnd, ?], M] = unreachable("deadEnd")
  implicit def read[A, M[_]:Monad]: Planner[Const[Read[A], ?], M] = unreachable("read")
  implicit def shiftedReadPath[M[_]:Monad]: Planner[Const[ShiftedRead[ADir], ?], M] = unreachable("shifted read of a dir")
  implicit def projectBucket[T[_[_]], M[_]:Monad]: Planner[ProjectBucket[T, ?], M] = unreachable("projectBucket")
  implicit def thetaJoin[T[_[_]], M[_]:Monad]: Planner[ThetaJoin[T, ?], M] = unreachable("thetajoin")
  implicit def shiftedread[M[_]:Monad]: Planner[Const[ShiftedRead[AFile], ?], M] = new ShiftedReadPlanner[M]
  implicit def qscriptCore[T[_[_]]: BirecursiveT: ShowT, M[_]:Monad]: Planner[QScriptCore[T, ?], M] = new QScriptCorePlanner[T, M]
  implicit def equiJoin[T[_[_]]: BirecursiveT: ShowT, M[_]:Monad]: Planner[EquiJoin[T, ?], M] = new EquiJoinPlanner[T, M]

  implicit def coproduct[F[_], G[_], M[_]:Monad](
    implicit F: Planner[F, M], G: Planner[G, M]):
      Planner[Coproduct[F, G, ?], M] =
    new Planner[Coproduct[F, G, ?], M] {
      def plan(fromFile: AFile => M[RDD[Data]], first: RDD[Data] => M[Data]): AlgebraM[SparkState[M, ?], Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile, first), G.plan(fromFile, first))
    }

  private def unreachable[F[_], M[_]:Monad](what: String): Planner[F, M] =
    new Planner[F, M] {
      def plan(fromFile: AFile => M[RDD[Data]], first: RDD[Data] => M[Data]): AlgebraM[SparkState[M, ?], F, RDD[Data]] =
        _ =>  StateT((sc: SparkContext) => {
          EitherT(InternalError.fromMsg(s"unreachable $what").left[(SparkContext, RDD[Data])].point[M])
        })
    }

}
