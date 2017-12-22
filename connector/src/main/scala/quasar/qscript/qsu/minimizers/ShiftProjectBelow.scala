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
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  SrcHole
}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.rewrites.NormalizableT
import slamdata.Predef._

import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.data.free._
import scalaz.{
  -\/,
  \/-,
  \/,
  Either3,
  Left3,
  Middle3,
  Monad,
  NonEmptyList => NEL,
  Right3,
  Scalaz
}, Scalaz._

final class ShiftProjectBelow[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private val func = construction.Func[T]
  private val N = new NormalizableT[T]

  private val accessHoleLeftF =
    Access.valueHole[T[EJson]](SrcHole).left[Int].point[FreeMapA]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case ConsecutiveLeftShifts(_, _) => true; case _ => false }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case ConsecutiveLeftShifts(src, shifts) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val reversed = shifts.reverse

        val rest = reversed.tail

        val initM = reversed.head match {
          case -\/(QSU.LeftShift(_, struct, idStatus, repair, rot)) =>
            val struct2 = struct.flatMap(κ(fm))

            val repair2 = repair flatMap {
              case QSU.AccessLeftTarget(Access.Value(_)) => fm.map[QSU.ShiftTarget[T]](κ(QSU.AccessLeftTarget[T](Access.valueHole(SrcHole))))
              case access@QSU.AccessLeftTarget(_) => (access: QSU.ShiftTarget[T]).pure[FreeMapA]
              case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
              case QSU.RightTarget() => func.RightTarget
            }

            updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, repair2, rot)) map { rewritten =>
              rewritten :++ src
            }

          case \/-(QSU.MultiLeftShift(_, shifts, repair)) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                (struct.flatMap(κ(fm)), idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                fm.map[QAccess[Hole] \/ Int](κ(-\/(access)))

              case \/-(idx) =>
                idx.right[QAccess[Hole]].point[FreeMapA]
            }

            updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts2, repair2)) map { rewritten =>
              rewritten :++ src
            }
        }

        for {
          init <- initM

          back <- rest.foldLeftM[G, QSUGraph](init) {
            case (src, -\/(QSU.LeftShift(_, struct, idStatus, repair, rot))) =>
              updateGraph[T, G](QSU.LeftShift(src.root, struct, idStatus, repair, rot)) map { rewritten =>
                rewritten :++ src
              }

            case (src, \/-(QSU.MultiLeftShift(_, shifts, repair))) =>
              updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts, repair)) map { rewritten =>
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

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      src: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    def coalesceUneven(shifts: NEL[QSU.LeftShift[T, QSUGraph] \/ QSU.MultiLeftShift[T, QSUGraph]], qgraph: QSUGraph): G[QSUGraph] = {
      val origFM = qgraph match {
        case Map(_, fm) => fm
        case _ => func.Hole
      }

      val reversed = shifts.reverse

      val initPattern = reversed.head match {
        case -\/(QSU.LeftShift(_, struct, idStatus, repair, rot)) =>
          val repair2 = func.ConcatMaps(
            func.MakeMapS("original", func.AccessLeftTarget(Access.valueHole[T[EJson]](_))),
            func.MakeMapS("results", repair))

          QSU.LeftShift[T, Symbol](src.root, struct, idStatus, repair2, rot)

        case \/-(QSU.MultiLeftShift(_, shifts, repair)) =>
          val repair2 = func.ConcatMaps(
            func.MakeMapS("original", accessHoleLeftF),
            func.MakeMapS("results", repair))

          QSU.MultiLeftShift[T, Symbol](src.root, shifts, repair2)
      }

      for {
        init2 <- updateGraph[T, G](initPattern)

        reconstructed <- reversed.tail.foldLeftM[G, QSUGraph](init2) {
          case (src, -\/(QSU.LeftShift(_, struct, idStatus, repair, rot))) =>
            val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "results")))

            val repair2 = repair flatMap {
              case QSU.AccessLeftTarget(Access.Value(_)) =>
                func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole[T[EJson]](_)), "results")
              case QSU.AccessLeftTarget(access) =>
                func.AccessLeftTarget(access.as(_))
              case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in ShiftProjectBelow")
              case QSU.RightTarget() => func.RightTarget
            }

            // we use right-biased map concat to our advantage here and overwrite results
            val repair3 = func.ConcatMaps(
              func.AccessLeftTarget(Access.valueHole[T[EJson]](_)),
              func.MakeMapS("results", repair2))

            updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct2, idStatus, repair3, rot)) map { rewritten =>
              rewritten :++ src
            }

          case (src, \/-(QSU.MultiLeftShift(_, shifts, repair))) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "results")))

                (struct2, idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                func.ProjectKeyS(access.left[Int].point[FreeMapA], "results")

              case \/-(idx) => idx.right[QAccess[Hole]].point[FreeMapA]
            }

            // we use right-biased map concat to our advantage here and overwrite results
            val repair3 = func.ConcatMaps(
              accessHoleLeftF,
              func.MakeMapS("results", repair2))

            updateGraph[T, G](QSU.MultiLeftShift[T, Symbol](src.root, shifts2, repair2)) map { rewritten =>
              rewritten :++ src
            }
        }

        rewritten = reconstructed match {
          case reconstructed @ LeftShift(src, struct, idStatus, repair, rot) =>
            val origLifted = origFM.flatMap(κ(
              func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole[T[EJson]](_)), "original")))

            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, "results"), origLifted)

            reconstructed.overwriteAtRoot(
              QSU.LeftShift(src.root, struct, idStatus, N.freeMF(repair2), rot))

          case reconstructed @ MultiLeftShift(src, shifts, repair) =>
            val origLifted = origFM.flatMap(κ(
              func.ProjectKeyS(accessHoleLeftF, "original")))

            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, "results"), origLifted)

            reconstructed.overwriteAtRoot(
              QSU.MultiLeftShift(src.root, shifts, repair2 /*N.freeMF(repair2)*/))

          case reconstructed => reconstructed
        }
      } yield qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten
    }

    for {
      wrapped <- candidates.zipWithIndex traverse {
        case (qgraph @ ConsecutiveLeftShifts(_, shifts), i) =>
          shifts.reverse.head match {
            case -\/(QSU.LeftShift(parent, struct, idStatus, repair, rotation)) =>
              qgraph.overwriteAtRoot(
                QSU.LeftShift(
                  parent.root,
                  struct,
                  idStatus,
                  func.MakeMapS(s"${src.root}_$i", repair),
                  rotation)).point[G]

            case \/-(QSU.MultiLeftShift(parent, shifts, repair)) =>
              qgraph.overwriteAtRoot(
                QSU.MultiLeftShift(
                  parent.root,
                  shifts,
                  func.MakeMapS(s"${src.root}_$i", repair))).point[G]
          }

        case (qgraph @ Map(parent, fm), i) =>
          qgraph.overwriteAtRoot(QSU.Map(parent.root, func.MakeMapS(i.toString, fm))).point[G]

        case (qgraph, i) =>
          updateGraph[T, G](QSU.Map(qgraph.root, func.MakeMapS(i.toString, func.Hole))) map { rewritten =>
            rewritten :++ qgraph
          }
      }

      coalesced <- wrapped.tail.foldLeftM[G, QSUGraph](wrapped.head) {
        case (ConsecutiveLeftShifts(_, shifts1), ConsecutiveLeftShifts(_, shifts2)) =>
          // we put everything together with fake parentage and fix it up later
          val zipped =
            zipMerge(shifts1.toList, shifts2.toList) {
              case
                (
                  -\/(QSU.LeftShift(fakeParent, structL, idStatusL, repairL, rotL)),
                  -\/(QSU.LeftShift(_, structR, idStatusR, repairR, rotR))) =>

                val repairLAdj = repairL map {
                  case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
                  case QSU.AccessLeftTarget(access) => access.left[Int]
                  case QSU.RightTarget() => 0.right[QAccess[Hole]]
                }

                val repairRAdj = repairR map {
                  case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
                  case QSU.AccessLeftTarget(access) => access.left[Int]
                  case QSU.RightTarget() => 1.right[QAccess[Hole]]
                }

                val repair =
                  func.ConcatMaps(
                    func.MakeMapS("0", repairLAdj),
                    func.MakeMapS("1", repairRAdj))

                QSU.MultiLeftShift[T, QSUGraph](
                  fakeParent,
                  (structL, idStatusL, rotL) :: (structR, idStatusR, rotR) :: Nil,
                  repair)

              case
                (
                  \/-(QSU.MultiLeftShift(fakeParent, shifts, repairL)),
                  -\/(QSU.LeftShift(_, structR, idStatusR, repairR, rotR))) =>

                val offset = shifts.length

                val repairRAdj = repairR map {
                  case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
                  case QSU.AccessLeftTarget(access) => access.left[Int]
                  case QSU.RightTarget() => offset.right[QAccess[Hole]]
                }

                val repair =
                  func.ConcatMaps(
                    func.MakeMapS("0", repairL),
                    func.MakeMapS("1", repairRAdj))

                QSU.MultiLeftShift[T, QSUGraph](
                  fakeParent,
                  shifts ::: (structR, idStatusR, rotR) :: Nil,
                  repair)

              case
                (
                  -\/(QSU.LeftShift(fakeParent, structL, idStatusL, repairL, rotL)),
                  \/-(QSU.MultiLeftShift(_, shifts, repairR))) =>

                val repairLAdj = repairL map {
                  case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
                  case QSU.AccessLeftTarget(access) => access.left[Int]
                  case QSU.RightTarget() => 0.right[QAccess[Hole]]
                }

                val repairRAdj = repairR.map(_.map(_ + 1))

                val repair =
                  func.ConcatMaps(
                    func.MakeMapS("0", repairLAdj),
                    func.MakeMapS("1", repairRAdj))

                QSU.MultiLeftShift[T, QSUGraph](
                  fakeParent,
                  (structL, idStatusL, rotL) :: shifts,
                  repair)

              case
                (
                  \/-(QSU.MultiLeftShift(fakeParent, shiftsL, repairL)),
                  \/-(QSU.MultiLeftShift(_, shiftsR, repairR))) =>

                val offset = shiftsL.length
                val repairRAdj = repairR.map(_.map(_ + offset))

                val repair =
                  func.ConcatMaps(
                    func.MakeMapS("0", repairL),
                    func.MakeMapS("1", repairRAdj))

                QSU.MultiLeftShift[T, QSUGraph](
                  fakeParent,
                  shiftsL ::: shiftsR,
                  repair)
            }

          val backM = zipped.foldLeftM[G, Option[QSUGraph]](None) {
            case (None, Middle3(QSU.MultiLeftShift(parent, shifts, repair))) =>
              updateGraph[T, G](QSU.MultiLeftShift(parent.root, shifts, repair)) map { rewritten =>
                Some(rewritten :++ parent)
              }

            // this can only happen if we somehow tried to zip a graph that contained no shifts
            case (None, _) => scala.sys.error("unreachable")

            case (Some(parent), Middle3(QSU.MultiLeftShift(_, shifts, repair))) =>
              val shifts2 = shifts.zipWithIndex map {
                case ((struct, idStatus, rot), i) =>
                  val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, i.toString)))
                  (struct2, idStatus, rot)
              }

              updateGraph[T, G](QSU.MultiLeftShift(parent.root, shifts2, repair)) map { rewritten =>
                Some(rewritten :++ parent)
              }

            case (Some(parent), Left3(-\/(QSU.LeftShift(_, struct, idStatus, repair, rot)))) =>
              val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "0")))

              val repair2 =
                func.ConcatMaps(
                  func.MakeMapS("0", repair),
                  func.MakeMapS("1",
                    func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole[T[EJson]](_)), "1")))

              updateGraph[T, G](QSU.LeftShift(parent.root, struct2, idStatus, repair2, rot)) map { rewritten =>
                Some(rewritten :++ parent)
              }

            case (Some(parent), Left3(\/-(QSU.MultiLeftShift(_, shifts, repair)))) =>
              val shifts2 = shifts map {
                case (struct, idStatus, rot) =>
                  val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "0")))
                  (struct2, idStatus, rot)
              }

              val repair2 =
                func.ConcatMaps(
                  func.MakeMapS("0", repair),
                  func.MakeMapS("1",
                    func.ProjectKeyS(accessHoleLeftF, "1")))

              updateGraph[T, G](QSU.MultiLeftShift(parent.root, shifts2, repair2)) map { rewritten =>
                Some(rewritten :++ parent)
              }

            case (Some(parent), Right3(-\/(QSU.LeftShift(_, struct, idStatus, repair, rot)))) =>
              val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "1")))

              val repair2 =
                func.ConcatMaps(
                  func.MakeMapS("0",
                    func.ProjectKeyS(func.AccessLeftTarget(Access.valueHole[T[EJson]](_)), "0")),
                  func.MakeMapS("1", repair))

              updateGraph[T, G](QSU.LeftShift(parent.root, struct2, idStatus, repair2, rot)) map { rewritten =>
                Some(rewritten :++ parent)
              }

            case (Some(parent), Right3(\/-(QSU.MultiLeftShift(_, shifts, repair)))) =>
              val shifts2 = shifts map {
                case (struct, idStatus, rot) =>
                  val struct2 = struct.flatMap(κ(func.ProjectKeyS(func.Hole, "1")))
                  (struct2, idStatus, rot)
              }

              val repair2 =
                func.ConcatMaps(
                  func.MakeMapS("0",
                    func.ProjectKeyS(accessHoleLeftF, "0")),
                  func.MakeMapS("1", repair))

              updateGraph[T, G](QSU.MultiLeftShift(parent.root, shifts2, repair2)) map { rewritten =>
                Some(rewritten :++ parent)
              }
          }

        // we know that the inner-inner structures are maps, because we did that when we created `wrapped`
        // so we unpack the residue of the pairwise coalescence ({"0": ..., "1": ...}) and concat
        backM map {
          case Some(qgraph @ MultiLeftShift(parent, shifts, repair)) =>
            val repair2 =
              func.ConcatMaps(
                func.ProjectKeyS(repair, "0"),
                func.ProjectKeyS(repair, "1"))

            val repairN = repair2 // N.freeMF(repair2)

            qgraph.overwriteAtRoot(QSU.MultiLeftShift(parent.root, shifts, repairN))

          case Some(qgraph @ LeftShift(parent, struct, idStatus, repair, rot)) =>
            val repair2 =
              func.ConcatMaps(
                func.ProjectKeyS(repair, "0"),
                func.ProjectKeyS(repair, "1"))

            val repairN = N.freeMF(repair2)

            qgraph.overwriteAtRoot(QSU.LeftShift(parent.root, struct, idStatus, repairN, rot))

          // this should only happen if we have an empty stack of shifts
          case _ => scala.sys.error("unreachable")
        }

        case (qgraph, ConsecutiveLeftShifts(_, shifts)) =>
          coalesceUneven(shifts, qgraph)

        case (ConsecutiveLeftShifts(_, shifts), qgraph) =>
          coalesceUneven(shifts, qgraph)

        // these two graphs have to be maps on the same thing
        // if they aren't, we're in trouble
        case (qgraph @ Map(parent1, left), Map(parent2, right)) =>
          scala.Predef.assert(parent1.root === parent2.root)

          qgraph.overwriteAtRoot(QSU.Map(parent1.root, func.ConcatMaps(left, right))).point[G]
      }

      // we build the map node to overwrite the original autojoin (qgraph)
      back = qgraph.overwriteAtRoot(
        QSU.Map(
          coalesced.root,
          fm.flatMap(i => func.ProjectKeyS(func.Hole, s"${src.root}_$i")))) :++ coalesced
    } yield Some((coalesced, back))
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  private def zipMerge[A, B, C](left: List[A \/ C], right: List[A \/ C])(f: (A \/ C, A \/ C) => B): List[Either3[A \/ C, B, A \/ C]] = {
    (left, right) match {
      case (headL :: tailL, headR :: tailR) =>
        Either3.middle3(f(headL, headR)) :: zipMerge(tailL, tailR)(f)

      case (head :: tail, Nil) =>
        Either3.left3(head) :: zipMerge(tail, Nil)(f)

      case (Nil, head :: tail) =>
        Either3.right3(head) :: zipMerge(Nil, tail)(f)

      case (Nil, Nil) =>
        Nil
    }
  }

  // TODO support freemappable and Cond-able regions between shifts
  object ConsecutiveLeftShifts {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def unapply(qgraph: QSUGraph)
        : Option[(QSUGraph, NEL[QSU.LeftShift[T, QSUGraph] \/ QSU.MultiLeftShift[T, QSUGraph]])] = qgraph match {

      case LeftShift(parent @ LeftShift(Read(_), _, _, _, _), struct, idStatus, repair, rot) =>
        Some((parent, NEL(-\/(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, repair, rot)))))

      case LeftShift(oparent @ ConsecutiveLeftShifts(parent, inners), struct, idStatus, repair, rot) =>
        Some((parent, -\/(QSU.LeftShift[T, QSUGraph](oparent, struct, idStatus, repair, rot)) <:: inners))

      case LeftShift(Read(_), _, _, _, _) =>
        None

      case LeftShift(parent, struct, idStatus, repair, rot) =>
        Some((parent, NEL(-\/(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, repair, rot)))))

      case MultiLeftShift(parent @ LeftShift(Read(_), _, _, _, _), shifts, rot) =>
        Some((parent, NEL(\/-(QSU.MultiLeftShift[T, QSUGraph](parent, shifts, rot)))))

      case MultiLeftShift(oparent @ ConsecutiveLeftShifts(parent, inners), shifts, rot) =>
        Some((parent, \/-(QSU.MultiLeftShift[T, QSUGraph](oparent, shifts, rot)) <:: inners))

      case MultiLeftShift(parent, shifts, rot) =>
        Some((parent, NEL(\/-(QSU.MultiLeftShift[T, QSUGraph](parent, shifts, rot)))))

      case _ =>
        None
    }
  }
}

object ShiftProjectBelow {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: ShiftProjectBelow[T] =
    new ShiftProjectBelow[T]
}
