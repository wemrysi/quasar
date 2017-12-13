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

import quasar.{NameGenerator, Planner, RenderTreeT}, Planner.PlannerErrorME
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  HoleF,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.rewrites.NormalizableT
import slamdata.Predef._

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._
import scalaz.{Monad, NonEmptyList => NEL, Scalaz}, Scalaz._

final class ShiftProjectBelow[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val func = construction.Func[T]

  private val N = new NormalizableT[T]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean = candidates match {
    case LeftShift(_, _, _, _, _) :: LeftShift(_, _, _, _, _) :: Nil => false

    case LeftShift(_, _, _, _, _) :: _ :: Nil => true
    case _ :: LeftShift(_, _, _, _, _) :: Nil => true

    case _ => false
  }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: MonadState_[?[_], RevIdx]: MonadState_[?[_], MinimizationState[T]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case ConsecutiveLeftShifts(src, shifts) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val reversed = shifts.reverse

        val QSU.LeftShift(_, struct, idStatus, repair, rot) = reversed.head
        val rest = reversed.tail

        val struct2 = struct.flatMap(κ(fm))

        val repair2 = repair flatMap {
          case QSU.AccessLeftTarget(Access.valueHole(_)) => fm.map[QSU.ShiftTarget](κ(QSU.AccessLeftTarget(Access.valueHole(SrcHole))))
          case access@QSU.AccessLeftTarget(_) => (access: QSU.ShiftTarget).pure[FreeMapA]
          case QSU.LeftTarget => scala.sys.error("QSU.LeftTarget in CollapseShifts")
          case QSU.RightTarget => func.RightTarget
        }

        for {
          init <-
            updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, repair2, rot)) map { rewritten =>
              rewritten :++ src
            }

          back <- rest.foldLeftM[G, QSUGraph](init) {
            case (src, QSU.LeftShift(_, struct, idStatus, repair, rot)) =>
              updateGraph[T, G](QSU.LeftShift(src.root, struct, idStatus, repair, rot)) map { rewritten =>
                rewritten :++ src
              }
          }
        } yield qgraph.overwriteAtRoot(back.vertices(back.root)) :++ back
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
      src: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val extraction = candidates match {
      case ConsecutiveLeftShifts(_, shifts) :: _ :: Nil => Some((shifts, true))
      case _ :: ConsecutiveLeftShifts(_, shifts) :: Nil => Some((shifts, false))
      case _ => None
    }

    extraction traverse {
      case (shifts, leftToRight) =>
        val reversed = shifts.reverse

        val fm2 = if (leftToRight)
          fm
        else
          fm.map(1 - _)   // we know the domain is {0, 1}, so we invert the indices

        val QSU.LeftShift(_, struct, idStatus, repair, rot) = reversed.head
        val rest = reversed.tail

        if (rest.isEmpty) {
          val repair2 = fm2 flatMap {
            case 0 => repair
            case 1 => func.AccessLeftTarget(Access.valueHole(_))
          }

          // TODO I'm pretty sure this messes up dimension names
          updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct, idStatus, repair2, rot)) map { rewritten =>
            val back = qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten

            (back, back)
          }
        } else {
          val repair2 = func.ConcatMaps(
            func.MakeMapS("original", func.AccessLeftTarget(Access.valueHole(_))),
            func.MakeMapS("results", repair))

          for {
            init2 <- updateGraph[T, G](
              QSU.LeftShift[T, Symbol](src.root, struct, idStatus, repair2, rot))

            reconstructed <- rest.foldLeftM[G, QSUGraph](init2) {
              case (src, QSU.LeftShift(_, struct, idStatus, repair, rot)) =>
                val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "results")))

                val repair2 = repair flatMap {
                  case QSU.AccessLeftTarget(Access.valueHole(_)) =>
                    func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole(_)), "results")
                  case QSU.LeftTarget => scala.sys.error("QSU.LeftTarget in ShiftProjectBelow")
                  case QSU.RightTarget => func.RightTarget
                }

                // we use right-biased map concat to our advantage here and overwrite results
                val repair3 = func.ConcatMaps(
                  func.AccessLeftTarget(Access.valueHole(_)),
                  func.MakeMapS("results", func.RightTarget))

                updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct2, idStatus, repair3, rot)) map { rewritten =>
                  rewritten :++ src
                }
            }

            // TODO this creates some unnecessary structure (we create the results map, only to deconstruct it again)
            rewritten = reconstructed match {
              case reconstructed @ LeftShift(src, struct, idStatus, repair, rot) =>
                val repair2 = fm2 flatMap {
                  case 0 => func.ProjectKeyS(repair, "results")
                  case 1 => func.ProjectKeyS(repair, "original")
                }

                val repairN = N.freeMF(repair2)

                reconstructed.overwriteAtRoot(
                  QSU.LeftShift(src.root, struct, idStatus, repairN, rot))

              case reconstructed => reconstructed
            }

            back = qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten
          } yield (back, back)
        }
    }
  }

  object ConsecutiveLeftShifts {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapply(qgraph: QSUGraph): Option[(QSUGraph, NEL[QSU.LeftShift[T, QSUGraph]])] = qgraph match {
      case LeftShift(oparent @ ConsecutiveLeftShifts(parent, inners), struct, idStatus, repair, rot) =>
        Some((parent, QSU.LeftShift[T, QSUGraph](oparent, struct, idStatus, repair, rot) <:: inners))

      case LeftShift(parent, struct, idStatus, repair, rot) =>
        Some((parent, NEL(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, repair, rot))))

      case _ =>
        None
    }
  }
}

object ShiftProjectBelow {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: ShiftProjectBelow[T] =
    new ShiftProjectBelow[T]
}
