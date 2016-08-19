/*
 * Copyright 2014–2016 SlamData Inc.
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

import simulacrum.typeclass
import quasar.Predef._
import quasar._
import quasar.Planner._
import quasar.fp._
import quasar.qscript._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

@typeclass trait Planner[F[_]] {
  type IT[G[_]]
  
  def plan[T[_[_]]]: AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], F, Planner.SparkStuff]
}

object Planner {
  type SparkStuff = RDD[Data]

  type Aux[T[_[_]], F[_]] = Planner[F] { type IT[G[_]] = T[G] }
  
  // NB: Shouldn’t need this once we convert to paths.
  implicit def deadEnd[T[_[_]]]: Planner.Aux[T, Const[DeadEnd, ?]] =
    new Planner[Const[DeadEnd, ?]] {
      type IT[G[_]] = T[G]
      def plan = ???
    }

  implicit def read[T[_[_]]]: Planner.Aux[T, Const[Read, ?]] =
    new Planner[Const[Read, ?]] {
      type IT[G[_]] = T[G]
      def plan =
        // is it?
        (qs: Const[Read, Read]) => {
          StateT((sc: SparkContext) => {
            val filePath = qs.getConst.path
            val rdd = sc.textFile(posixCodec.unsafePrintPath(filePath))
            (sc, rdd).right[PlannerError]
          })
        }
    }
  
  implicit def sourcedPathable[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, SourcedPathable[T, ?]] =
    new Planner[SourcedPathable[T, ?]] {
      type IT[G[_]] = T[G]
      def plan: AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], SourcedPathable[T,?], Planner.SparkStuff] = {
        case LeftShift(src, struct, repair) => ???
        case Union(src, lBranch, rBranch) => ???
      }
    }
  
  implicit def qscriptCore[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      type IT[G[_]] = T[G]
      def plan: AlgebraM[StateT[PlannerError \/ ?, SparkContext, ?], QScriptCore[T, ?], Planner.SparkStuff] = {
        case qscript.Map(src, f) =>
          ???
        case Reduce(src, bucket, reducers, repair) =>
          ???
        case Sort(src, bucket, order) =>
          ???
        case Filter(src, f) =>
          ???
        case Take(src, from, count) =>
          ???
        case Drop(src, from, count) =>
          ???
      }
    }
  
  implicit def equiJoin[T[_[_]]: Recursive: ShowT]:
      Planner.Aux[T, EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan = ???      
    }
  
  // TODO: Remove this instance
  implicit def thetaJoin[T[_[_]]]: Planner.Aux[T, ThetaJoin[T, ?]] =
    new Planner[ThetaJoin[T, ?]] {
      type IT[G[_]] = T[G]
      def plan = ???
    }
  
  // TODO: Remove this instance
  implicit def projectBucket[T[_[_]]]: Planner.Aux[T, ProjectBucket[T, ?]] =
    new Planner[ProjectBucket[T, ?]] {
      type IT[G[_]] = T[G]
      def plan = ???
    }
  
  implicit def coproduct[T[_[_]], F[_], G[_]](
    implicit F: Planner.Aux[T, F], G: Planner.Aux[T, G]):
      Planner.Aux[T, Coproduct[F, G, ?]] =
    new Planner [Coproduct[F, G, ?]] {
      type IT[G[_]] = T[G]
      def plan = _.run.fold(F.plan, G.plan)
    }
}
