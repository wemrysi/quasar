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
  IdStatus,
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
  Free,
  Monad,
  NonEmptyList => NEL,
  Scalaz
}, Scalaz._

final class CollapseShifts[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  private type ShiftGraph = QSU.LeftShift[T, QSUGraph] \/ QSU.MultiLeftShift[T, QSUGraph]

  private val func = construction.Func[T]
  private val N = new NormalizableT[T]

  private val accessHoleLeftF =
    Access.valueHole[T[EJson]](SrcHole).left[Int].point[FreeMapA]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case ConsecutiveUnbounded(_, _) => true; case _ => false }

  def extract[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case ConsecutiveUnbounded(src, shifts) =>
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
        } yield back
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
            rewritten :++ src
          }
        }
      }

      Some((qgraph, rebuild _))
  }

  /*
   * Builds some intermediate structure:
   *
   * - Results are wrapped in { "0": ..., "1": ..., etc }, matching the index of
   *   the inbound candidate and thus also the reference within fm.  The root will
   *   be overwritten with a Map that applies fm to ProjectKeyS(Hole, i.toString).
   *   This wrapping is done before any of the coalescences are considered, and the
   *   candidate shifts are corrected to ensure validity and consistency in light of
   *   this wrapping.
   *
   * - In the event of a flat coalescence with multiple shifts (e.g. `a[*][*] + b`),
   *   the original upstream value (Hole of the upper struct) will be wrapped in
   *   { "original": ... }, while the result at each stage will be wrapped in
   *   { "results": ... }, with both maps concatenated together in the repair.
   *
   * - In the event of a multi-coalesce (e.g. a[*] + b[*]), the coalescence is handled
   *   pair-wise with the left and right sides wrapped in { "left": ... } and
   *   { "right": ... }, respectively.  This allows each side to continue the shift chain
   *   in the event that each side has several stages.  It also allows access to a
   *   terminal value in the event that the chains are uneven (e.g. a[*][*] + b[*]).
   *
   * These wrappings compose together, but the second two should be entirely invisible
   * in the final output.  In other words, the final output should simply be wrapped in
   * the index object, enabling the final Map node to overwrite at root.  The second
   * and third wrappings are simply for bookkeeping purposes within each coalescence.
   */
  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def apply[
      G[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM[T, ?[_]]: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      src: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val ConsecutiveBounded = ConsecutiveLeftShifts(_.root === src.root)

    def coalesceUneven(shifts: NEL[ShiftGraph], qgraph: QSUGraph): G[QSUGraph] = {
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
              case QSU.LeftTarget() => scala.sys.error("QSU.LeftTarget in CollapseShifts")
              case QSU.RightTarget() => func.RightTarget
            }

            // we use right-biased map concat to our advantage here and overwrite results
            val repair3 = func.ConcatMaps(
              func.MakeMapS(
                "original",
                func.ProjectKeyS(
                  func.AccessLeftTarget(Access.valueHole[T[EJson]](_)),
                  "original")),
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
              func.MakeMapS(
                "original",
                func.ProjectKeyS(accessHoleLeftF, "original")),
              func.MakeMapS("results", repair2))

            updateGraph[T, G](QSU.MultiLeftShift[T, Symbol](src.root, shifts2, repair2)) map { rewritten =>
              rewritten :++ src
            }
        }

        rewritten = reconstructed match {
          case reconstructed @ LeftShift(src, struct, idStatus, repair, rot) =>
            val origLifted = origFM.flatMap(κ(func.ProjectKeyS(repair, "original")))
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, "results"), origLifted)

            reconstructed.overwriteAtRoot(
              QSU.LeftShift(src.root, struct, idStatus, N.freeMF(repair2), rot))

          case reconstructed @ MultiLeftShift(src, shifts, repair) =>
            val origLifted = origFM.flatMap(κ(func.ProjectKeyS(repair, "original")))
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, "results"), origLifted)

            reconstructed.overwriteAtRoot(
              QSU.MultiLeftShift(src.root, shifts, repair2 /*N.freeMF(repair2)*/))

          case reconstructed => reconstructed
        }
      } yield qgraph.overwriteAtRoot(rewritten.vertices(rewritten.root)) :++ rewritten
    }

    def coalesceZip(left: List[ShiftGraph], right: List[ShiftGraph], parent: Option[QSUGraph]): G[QSUGraph] = {
      val hasParent = parent.isDefined

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def continue(
          fakeParent: QSUGraph,
          tailL: List[ShiftGraph],
          tailR: List[ShiftGraph])(
          pat: Symbol => QSU[T, Symbol]): G[QSUGraph] = {

        val realParent = parent.getOrElse(fakeParent)
        val cont = updateGraph[T, G](pat(realParent.root)) map { rewritten =>
          Some(rewritten :++ realParent)
        }

        cont.flatMap(coalesceZip(tailL, tailR, _))
      }

      def name(isLeft: Boolean) = if (isLeft) "left" else "right"

      def fixSingleStruct(struct: FreeMap, isLeft: Boolean): FreeMap = {
        if (hasParent)
          struct.flatMap(κ(func.ProjectKeyS(func.Hole, name(isLeft))))
        else
          struct
      }

      def fixSingleRepairForMulti(
          repair: FreeMapA[QSU.ShiftTarget[T]],
          offset: Int,
          isLeft: Boolean): FreeMapA[QAccess[Hole] \/ Int] = {

        repair flatMap {
          case QSU.LeftTarget() =>
            scala.sys.error("QSU.LeftTarget in CollapseShifts")

          case QSU.AccessLeftTarget(access) =>
            val hole = Free.pure[MapFunc, QAccess[Hole] \/ Int](access.left[Int])

            if (hasParent)
              func.ProjectKeyS(hole, name(isLeft))
            else
              hole

          case QSU.RightTarget() =>
            Free.pure(offset.right[QAccess[Hole]])
        }
      }

      def fixSingleRepairForSingle(
          repair: FreeMapA[QSU.ShiftTarget[T]],
          isLeft: Boolean): FreeMapA[QSU.ShiftTarget[T]] = {

        repair flatMap {
          case target @ (QSU.LeftTarget() | QSU.RightTarget()) =>
            Free.pure(target)

          case access @ QSU.AccessLeftTarget(_) =>
            val hole = Free.pure[MapFunc, QSU.ShiftTarget[T]](access)

            if (hasParent)
              func.ProjectKeyS(hole, name(isLeft))
            else
              hole
        }
      }

      def fixMultiShifts(
          shifts: List[(FreeMap, IdStatus, QSU.Rotation)],
          isLeft: Boolean): List[(FreeMap, IdStatus, QSU.Rotation)] = {

        shifts map {
          case (struct, idStatus, rot) =>
            val struct2 = if (hasParent)
              struct.flatMap(κ(func.ProjectKeyS(func.Hole, name(isLeft))))
            else
              struct

            (struct2, idStatus, rot)
        }
      }

      def fixMultiRepair(
          repair: FreeMapA[QAccess[Hole] \/ Int],
          offset: Int,
          isLeft: Boolean): FreeMapA[QAccess[Hole] \/ Int] = {

        repair flatMap {
          case -\/(access) =>
            if (hasParent)
              func.ProjectKeyS(Free.pure(access.left[Int]), name(isLeft))
            else
              Free.pure(access.left[Int])

          case \/-(i) =>
            Free.pure((i + offset).right[QAccess[Hole]])
        }
      }

      (left, right) match {
        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, repairL, rotL)) :: tailL,
            -\/(QSU.LeftShift(_, structR, idStatusR, repairR, rotR)) :: tailR) =>

          val structLAdj = fixSingleStruct(structL, true)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, true)

          val structRAdj = fixSingleStruct(structR, false)
          val repairRAdj = fixSingleRepairForMulti(repairR, 1, false)

          val repair =
            func.ConcatMaps(
              func.MakeMapS("left", repairLAdj),
              func.MakeMapS("right", repairRAdj))

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structLAdj, idStatusL, rotL) :: (structRAdj, idStatusR, rotR) :: Nil,
              repair)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, repairL)) :: tailL,
            -\/(QSU.LeftShift(_, structR, idStatusR, repairR, rotR)) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, true)
          val repairLAdj = fixMultiRepair(repairL, 0, true)

          val structRAdj = fixSingleStruct(structR, false)
          val repairRAdj = fixSingleRepairForMulti(repairR, offset, false)

          val repair =
            func.ConcatMaps(
              func.MakeMapS("left", repairLAdj),
              func.MakeMapS("right", repairRAdj))

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: (structRAdj, idStatusR, rotR) :: Nil,
              repair)
          }

        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, repairL, rotL)) :: tailL,
            \/-(QSU.MultiLeftShift(_, shiftsR, repairR)) :: tailR) =>

          val structLAdj = fixSingleStruct(structL, true)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, true)

          val shiftsRAdj = fixMultiShifts(shiftsR, false)
          val repairRAdj = fixMultiRepair(repairR, 1, false)

          val repair =
            func.ConcatMaps(
              func.MakeMapS("left", repairLAdj),
              func.MakeMapS("right", repairRAdj))

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structL, idStatusL, rotL) :: shiftsRAdj,
              repair)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, repairL)) :: tailL,
            \/-(QSU.MultiLeftShift(_, shiftsR, repairR)) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, true)
          val repairLAdj = fixMultiRepair(repairL, 0, true)

          val shiftsRAdj = fixMultiShifts(shiftsR, false)
          val repairRAdj = fixMultiRepair(repairR, offset, false)

          val repair =
            func.ConcatMaps(
              func.MakeMapS("left", repairLAdj),
              func.MakeMapS("right", repairRAdj))

          continue(fakeParent, tailL, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: shiftsRAdj,
              repair)
          }

        case
          (
            -\/(QSU.LeftShift(fakeParent, structL, idStatusL, repairL, rotL)) :: tailL,
            Nil) =>

          val structLAdj = fixSingleStruct(structL, true)
          val repairLAdj = fixSingleRepairForSingle(repairL, true)

          val repair = func.ConcatMaps(
            func.MakeMapS("left", repairLAdj),
            func.MakeMapS(
              "right",
              if (hasParent)
                func.ProjectKeyS(func.AccessLeftTarget(Access.value(_)), "right")
              else
                func.AccessLeftTarget(Access.value(_))))

          continue(fakeParent, tailL, Nil) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              structLAdj,
              idStatusL,
              repair,
              rotL)
          }

        case
          (
            \/-(QSU.MultiLeftShift(fakeParent, shiftsL, repairL)) :: tailL,
            Nil) =>

          val shiftsLAdj = fixMultiShifts(shiftsL, true)
          val repairLAdj = fixMultiRepair(repairL, 0, true)

          val repair = func.ConcatMaps(
            func.MakeMapS("left", repairLAdj),
            func.MakeMapS(
              "right",
              if (hasParent)
                func.ProjectKeyS(func.AccessHole.map(_.left[Int]), "right")
              else
                func.AccessHole.map(_.left[Int])))

          continue(fakeParent, tailL, Nil) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj,
              repair)
          }

        case
          (
            Nil,
            -\/(QSU.LeftShift(fakeParent, structR, idStatusR, repairR, rotR)) :: tailR) =>

          val structRAdj = fixSingleStruct(structR, false)
          val repairRAdj = fixSingleRepairForSingle(repairR, false)

          val repair = func.ConcatMaps(
            func.MakeMapS(
              "left",
              if (hasParent)
                func.ProjectKeyS(func.AccessLeftTarget(Access.value(_)), "left")
              else
                func.AccessLeftTarget(Access.value(_))),
            func.MakeMapS("right", repairRAdj))

          continue(fakeParent, Nil, tailR) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              structRAdj,
              idStatusR,
              repair,
              rotR)
          }

        case
          (
            Nil,
            \/-(QSU.MultiLeftShift(fakeParent, shiftsR, repairR)) :: tailR) =>

          val shiftsRAdj = fixMultiShifts(shiftsR, true)
          val repairRAdj = fixMultiRepair(repairR, 0, true)

          val repair = func.ConcatMaps(
            func.MakeMapS(
              "left",
              if (hasParent)
                func.ProjectKeyS(func.AccessHole.map(_.left[Int]), "left")
              else
                func.AccessHole.map(_.left[Int])),
            func.MakeMapS("right", repairRAdj))

          continue(fakeParent, Nil, tailR) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsRAdj,
              repair)
          }

        case (Nil, Nil) =>
          // if parent is None here, it means we invoked on empty lists
          val realParent = parent.getOrElse(???)

          // we know that the contents are maps like { "0": ... }, etc
          val fm = func.ConcatMaps(
            func.ProjectKeyS(func.Hole, "left"),
            func.ProjectKeyS(func.Hole, "right"))

          updateGraph[T, G](QSU.Map(realParent.root, fm)) map { rewritten =>
            rewritten :++ realParent
          }
      }
    }

    for {
      // converts all candidates to produce final results wrapped in their relevant indices
      wrapped <- candidates.zipWithIndex traverse {
        case (qgraph @ ConsecutiveBounded(_, shifts), i) =>
          // qgraph must beLike(shifts.head)

          // shifts.head is the LAST shift in the chain
          shifts.head match {
            case -\/(QSU.LeftShift(parent, struct, idStatus, repair, rotation)) =>
              val pat = QSU.LeftShift(
                parent.root,
                struct,
                idStatus,
                func.MakeMapS(i.toString, repair),
                rotation)

              qgraph.overwriteAtRoot(pat).point[G]

            case \/-(QSU.MultiLeftShift(parent, shifts, repair)) =>
              val pat = QSU.MultiLeftShift(
                parent.root,
                shifts,
                func.MakeMapS(i.toString, repair))

              qgraph.overwriteAtRoot(pat).point[G]
          }

        case (qgraph @ Map(parent, fm), i) =>
          qgraph.overwriteAtRoot(QSU.Map(parent.root, func.MakeMapS(i.toString, fm))).point[G]

        case (qgraph, i) =>
          updateGraph[T, G](QSU.Map(qgraph.root, func.MakeMapS(i.toString, func.Hole))) map { rewritten =>
            rewritten :++ qgraph
          }
      }

      coalesced <- wrapped.tail.foldLeftM[G, QSUGraph](wrapped.head) {
        case (ConsecutiveBounded(_, shifts1), ConsecutiveBounded(_, shifts2)) =>
          coalesceZip(shifts1.toList.reverse, shifts2.toList.reverse, None)

        case (qgraph, ConsecutiveBounded(_, shifts)) =>
          coalesceUneven(shifts, qgraph)

        case (ConsecutiveBounded(_, shifts), qgraph) =>
          coalesceUneven(shifts, qgraph)

        // these two graphs have to be maps on the same thing
        // if they aren't, we're in trouble
        case (Map(parent1, left), Map(parent2, right)) =>
          scala.Predef.assert(parent1.root === parent2.root)

          updateGraph[T, G](QSU.Map(parent1.root, func.ConcatMaps(left, right))) map { rewritten =>
            rewritten :++ parent1
          }
      }

      // we build the map node to overwrite the original autojoin (qgraph)
      back = qgraph.overwriteAtRoot(
        QSU.Map(
          coalesced.root,
          fm.flatMap(i => func.ProjectKeyS(func.Hole, i.toString)))) :++ coalesced
    } yield Some((coalesced, back))
  }

  private val ConsecutiveUnbounded = ConsecutiveLeftShifts(κ(false))

  // TODO support freemappable and Cond-able regions between shifts
  private def ConsecutiveLeftShifts(stop: QSUGraph => Boolean): Extractor = new Extractor {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    lazy val Self = ConsecutiveLeftShifts(stop)

    def unapply(qgraph: QSUGraph)
        : Option[(QSUGraph, NEL[ShiftGraph])] = qgraph match {

      case qgraph if stop(qgraph) =>
        None

      case LeftShift(Read(_), _, _, _, _) =>
        None

      case LeftShift(oparent @ Self(parent, inners), struct, idStatus, repair, rot) =>
        Some((parent, -\/(QSU.LeftShift[T, QSUGraph](oparent, struct, idStatus, repair, rot)) <:: inners))

      case LeftShift(parent, struct, idStatus, repair, rot) =>
        Some((parent, NEL(-\/(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, repair, rot)))))

      case MultiLeftShift(oparent @ Self(parent, inners), shifts, rot) =>
        Some((parent, \/-(QSU.MultiLeftShift[T, QSUGraph](oparent, shifts, rot)) <:: inners))

      case MultiLeftShift(parent, shifts, rot) =>
        Some((parent, NEL(\/-(QSU.MultiLeftShift[T, QSUGraph](parent, shifts, rot)))))

      case _ =>
        None
    }
  }

  private trait Extractor {
    def unapply(qgraph: QSUGraph) : Option[(QSUGraph, NEL[ShiftGraph])]
  }
}

object CollapseShifts {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: CollapseShifts[T] =
    new CollapseShifts[T]
}
