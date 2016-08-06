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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.Planner.PlannerError
import quasar.qscript, qscript._, MapFuncs._

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

object Planner {

  def mapFunc[T[_[_]]]: Algebra[MapFunc[T, ?], String] = {
    case v @ ToString(a1) => ???
    case v => s" ???(MapFunc - $v)??? "
  }

  // TODO:
  //   why is hylo necessary here over just cata?
  //   why is hylo defined over free but not cata?
  //   what is the intuition for CoEnv.freeIso?
  // def freeCata[F[_]: Traverse, E, A](free: Free[F, E])(φ: Algebra[CoEnv[E, F, ?], A]) =
  //   free.hylo(φ, CoEnv.freeIso[E, F].reverseGet)

  // def getSqlFn[T[_[_]]: Recursive]: FreeMap[T] => String = freeMap =>
  //   freeCata(freeMap)(interpret(κ("defaultname"), javascript))

  type MarkLogicPlanner[QS[_]] = Planner[QS, String]

  trait Planner[QS[_], A] {
    def plan: AlgebraM[PlannerError \/ ?, QS, A]
  }
  object Planner {

    def apply[QS[_],A](implicit ev: Planner[QS,A]): Planner[QS,A] = ev

    implicit def coproduct[A, F[_]: Planner[?[_], A], G[_]: Planner[?[_], A]]: Planner[Coproduct[F, G, ?], A] =
      new Planner[Coproduct[F, G, ?], A] {
        def plan: AlgebraM[PlannerError \/ ?, Coproduct[F, G, ?], A] =
          _.run.fold(Planner[F,A].plan, Planner[G,A].plan)
      }
  }

  implicit def qscriptCore[T[_[_]]]: MarkLogicPlanner[QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?], String] {
      val plan: AlgebraM[PlannerError \/ ?, QScriptCore[T, ?], String] = {
        case qscript.Map(src, f)                           => ???
        case qscript.Reduce(src, bucket, reducers, repair) => ???
        case qscript.Sort(src, bucket, order)              => ???
        case qscript.Filter(src, f)                        => s"cts:and-query(($src, $f))".right
        case qscript.Take(src, from, count)                =>
          def from0 = ""
          def count0 = ""
          s"fn:subsequence($src, $from0, $count0)".right
        case qscript.Drop(src, from, count)                =>
          def from0 = ""
          def count0 = ""
          s"(fn:subsequence($src, 1, $from0), fn:subsequence($src, $from0 + $count0))".right
      }
    }

  implicit def sourcedPathable[T[_[_]]]: MarkLogicPlanner[SourcedPathable[T, ?]] =
    new Planner[SourcedPathable[T, ?], String] {
      val plan: AlgebraM[PlannerError \/ ?, SourcedPathable[T, ?], String] = {
        case LeftShift(src, struct, repair) => ???
        case Union(src, lBranch, rBranch) => ???
      }
    }

  implicit def constDeadEnd: MarkLogicPlanner[Const[DeadEnd, ?]] =
    new MarkLogicPlanner[Const[DeadEnd, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, Const[DeadEnd, ?], String] = {
        case Const(Root) => ??? //" null ".right
      }
    }

  implicit def constRead: MarkLogicPlanner[Const[Read, ?]] =
    new MarkLogicPlanner[Const[Read, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, Const[Read, ?], String] = {
        case Const(Read(absFile)) =>
          val asDir = fileParent(absFile) </> dir(fileName(absFile).value)
          val dirRepr = posixCodec.printPath(asDir)
          s"""cts:directory-query("$dirRepr","1")""".right
      }
    }

  implicit def projectBucket[T[_[_]]]: MarkLogicPlanner[ProjectBucket[T, ?]] =
    new MarkLogicPlanner[ProjectBucket[T, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, ProjectBucket[T, ?], String] = {
        case BucketField(src, value, name)  => ???
        case BucketIndex(src, value, index) => ???
      }
    }

  implicit def thetajoin[T[_[_]]]: MarkLogicPlanner[ThetaJoin[T, ?]] =
    new MarkLogicPlanner[ThetaJoin[T, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, ThetaJoin[T, ?], String] = {
        case ThetaJoin(src, lBranch, rBranch, on, f, combine) => ???
      }
    }

  implicit def equiJoin[T[_[_]]]: MarkLogicPlanner[EquiJoin[T, ?]] =
    new MarkLogicPlanner[EquiJoin[T, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, EquiJoin[T, ?], String] = {
        case EquiJoin(src, lBranch, rBranch, leftKey, rightKey, joinType, combineFunc) => ???
      }
    }

}
