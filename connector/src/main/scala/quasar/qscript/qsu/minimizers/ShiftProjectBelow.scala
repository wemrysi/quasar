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

package quasar.qscript.qsu
package minimizers

import quasar.{NameGenerator, Planner}, Planner.PlannerErrorME
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  HoleF,
  JoinSide,
  LeftSide,
  LeftSideF,
  RightSide,
  RightSideF
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef._

import matryoshka.{delayEqual, BirecursiveT, EqualT}
import matryoshka.data.free._
import scalaz.{Monad, Scalaz}, Scalaz._

final class ShiftProjectBelow[T[_[_]]: BirecursiveT: EqualT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  def couldApplyTo(candidates: List[QSUGraph]): Boolean = candidates match {
    case LeftShift(_, _, _, _, _) :: _ :: Nil => true
    case _ :: LeftShift(_, _, _, _, _) :: Nil => true
    case _ => false
  }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case qgraph @ LeftShift(src, struct, idStatus, repair, rot) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val struct2 = struct.flatMap(κ(fm))

        val repair2 = repair flatMap {
          case LeftSide => fm.map(κ(LeftSide: JoinSide))
          case RightSide => RightSideF[T]
        }

        updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, repair2, rot)) map { rewritten =>
          qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten :++ src
        }
      }

      Some((src, rebuild _))

    // we're potentially defined for all sides so long as there's a left shift around
    case qgraph =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        if (fm === HoleF[T]) {
          src.point[G]
        } else {
          // this case should never happen
          updateGraph[T, G](QSU.Map(src.root, fm)) map { rewritten =>
            qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten :++ src
          }
        }
      }

      Some((qgraph, rebuild _))
  }

  def apply[
      G[_]: Monad: NameGenerator: PlannerErrorME: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]](
      qgraph: QSUGraph,
      singleSource: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val extraction = candidates match {
      case LeftShift(src1, struct, idStatus, repair, rot) :: src2 :: Nil if src1.root === src2.root =>
        Some((src1, struct, idStatus, repair, rot, true))

      case src1 :: LeftShift(src2, struct, idStatus, repair, rot) :: Nil if src1.root === src2.root =>
        Some((src1, struct, idStatus, repair, rot, false))

      case _ => None
    }

    extraction traverse {
      case (src, struct, idStatus, repair, rot, leftToRight) =>
        val fm2 = if (leftToRight)
          fm
        else
          fm.map(1 - _)   // we know the domain is {0, 1}, so we invert the indices

        val repair2 = fm2 flatMap {
          case 0 => repair
          case 1 => LeftSideF[T]
        }

        updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct, idStatus, repair2, rot)) map { rewritten =>
          val back = qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten

          (back, back)
        }
    }
  }
}

object ShiftProjectBelow {
  def apply[T[_[_]]: BirecursiveT: EqualT]: ShiftProjectBelow[T] =
    new ShiftProjectBelow[T]
}
