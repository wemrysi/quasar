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

package quasar.qsu
package minimizers

import slamdata.Predef._
import quasar.{IdStatus, RenderTreeT}, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.common.effect.NameGenerator
import quasar.contrib.std.errorImpossible
import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.ski.κ
import quasar.qscript.{
  construction,
  Hole,
  HoleF,
  JoinSide,
  LeftSide,
  MFC,
  MFD,
  MapFuncCore,
  MapFuncsCore,
  MapFuncsDerived,
  MonadPlannerErr,
  OnUndefined,
  RightSide,
  SrcHole
}
import quasar.qscript.RecFreeS._
import quasar.qscript.provenance.Dimensions
import quasar.qsu.{QScriptUniform => QSU}, QSU.ShiftTarget

import matryoshka.{Recursive, Corecursive}
import matryoshka.{delayEqual, BirecursiveT, EqualT, ShowT}
import matryoshka.{delayShow, showTShow}
import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import scalaz.{
  -\/,
  \/-,
  \/,
  Equal,
  Free,
  Monad,
  NonEmptyList => NEL,
  Scalaz,
  Show
}, Scalaz._

final class CollapseShifts[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] private () extends Minimizer[T] {
  import MinimizeAutoJoins._
  import QSUGraph.Extractors._

  type D = T[EJson]

  private[this] val accO = Access.Optics[D]
  private[this] val qprov = QProv[T]
  private[this] val QPZero = Dimensions.origin(qprov.prov.fresh())

  import qprov.prov.implicits._

  private[this] sealed trait ShiftGraph extends Product with Serializable {

    // lol...
    def bimap[A, B](
        leftF: QSU.LeftShift[T, QSUGraph] => A,
        rightF: QSU.MultiLeftShift[T, QSUGraph] => B): A \/ B = this match {
      case ShiftGraph.Single(p, _) => -\/(leftF(p))
      case ShiftGraph.Multi(p, _) => \/-(rightF(p))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private[this] object ShiftGraph {
    case class Single(pattern: QSU.LeftShift[T, QSUGraph], dims: QDims) extends ShiftGraph
    case class Multi(pattern: QSU.MultiLeftShift[T, QSUGraph], dims: List[QDims]) extends ShiftGraph

    implicit val show: Show[ShiftGraph] = Show shows {
      case Single(pattern, _) => s"Single(${QScriptUniform.traverse[T].map(pattern)(_.root).shows})"
      case Multi(pattern, _) => s"Multi(${QScriptUniform.traverse[T].map(pattern)(_.root).shows})"
    }
  }

  private val func = construction.Func[T]
  private val recFunc = construction.RecFunc[T]

  private val ResultsField = "results"
  private val OriginalField = "original"

  private val LeftField = "left"
  private val RightField = "right"

  private val accessHoleLeftF =
    accO.value[Hole](SrcHole).left[Int].point[FreeMapA]

  def couldApplyTo(candidates: List[QSUGraph]): Boolean =
    candidates exists { case ConsecutiveUnbounded(_, _) => true; case _ => false }

  def extract[
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph): Option[(QSUGraph, (QSUGraph, FreeMap) => G[QSUGraph])] = qgraph match {

    case ConsecutiveUnbounded(src, shifts) =>
      def rebuild(src: QSUGraph, fm: FreeMap): G[QSUGraph] = {
        val reversed = shifts.reverse

        val rest = reversed.tail

        val initM = reversed.head match {
          case ShiftGraph.Single(QSU.LeftShift(_, struct, idStatus, onUndefined, repair, rot), _) =>
            val struct2 = struct >> fm.asRec

            val repair2 = repair flatMap {
              case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
                fm.map[ShiftTarget[D]](
                  κ(ShiftTarget.AccessLeftTarget(accO.value(SrcHole))))

              case access @ ShiftTarget.AccessLeftTarget(_) =>
                (access: ShiftTarget[D]).pure[FreeMapA]

              case ShiftTarget.LeftTarget() =>
                scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

              case ShiftTarget.RightTarget() =>
                RightTarget[T]
            }

            updateGraph[T, G](QSU.LeftShift(src.root, struct2, idStatus, onUndefined, repair2, rot)) map { rewritten =>
              rewritten :++ src
            }

          case ShiftGraph.Multi(QSU.MultiLeftShift(_, shifts, onUndefined, repair), _) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                (struct >> fm, idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                fm.map[QAccess[Hole] \/ Int](κ(-\/(access)))

              case \/-(idx) =>
                idx.right[QAccess[Hole]].point[FreeMapA]
            }

            updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts2, onUndefined, repair2)) map { rewritten =>
              rewritten :++ src
            }
        }

        for {
          init <- initM

          back <- rest.foldLeftM[G, QSUGraph](init) {
            case (src, ShiftGraph.Single(QSU.LeftShift(_, struct, idStatus, onUndefined, repair, rot), _)) =>
              updateGraph[T, G](QSU.LeftShift(src.root, struct, idStatus, onUndefined, repair, rot)) map { rewritten =>
                rewritten :++ src
              }

            case (src, ShiftGraph.Multi(QSU.MultiLeftShift(_, shifts, onUndefined, repair), _)) =>
              updateGraph[T, G](QSU.MultiLeftShift(src.root, shifts, onUndefined, repair)) map { rewritten =>
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
          updateGraph[T, G](QSU.Map(src.root, fm.asRec)) map { rewritten =>
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
      G[_]: Monad: NameGenerator: MonadPlannerErr: RevIdxM: MinStateM[T, ?[_]]](
      qgraph: QSUGraph,
      src: QSUGraph,
      candidates: List[QSUGraph],
      fm: FreeMapA[Int]): G[Option[(QSUGraph, QSUGraph)]] = {

    val ConsecutiveBounded = ConsecutiveLeftShifts(_.root === src.root)

    def mergeIndexMaps(parent: QSUGraph, leftHole: FreeMap, rightHole: FreeMap): G[QSUGraph] =
      updateGraph[T, G](QSU.Map(parent.root, recFunc.ConcatMaps(leftHole.asRec, rightHole.asRec))) map { rewritten =>
        rewritten :++ parent
      }

    def coalesceUneven(shifts: NEL[ShiftGraph], qgraph: QSUGraph): G[QSUGraph] = {
      val origFM = qgraph match {
        case Map(_, fm) => fm
        case _ => recFunc.Hole
      }

      val reversed = shifts.reverse

      val initPattern = reversed.head match {
        case ShiftGraph.Single(QSU.LeftShift(_, struct, idStatus, _, repair, rot), _) =>
          val repair2 = func.StaticMapS(
            OriginalField -> AccessLeftTarget[T](accO.value(_)),
            ResultsField -> repair)

          QSU.LeftShift[T, Symbol](src.root, struct, idStatus, OnUndefined.Emit, repair2, rot)

        case ShiftGraph.Multi(QSU.MultiLeftShift(_, shifts, _, repair), _) =>
          val repair2 = func.StaticMapS(
            OriginalField -> accessHoleLeftF,
            ResultsField -> repair)

          QSU.MultiLeftShift[T, Symbol](src.root, shifts, OnUndefined.Emit, repair2)
      }

      for {
        init2 <- updateGraph[T, G](initPattern) map (pat => pat :++ src)

        reconstructed <- reversed.tail.foldLeftM[G, QSUGraph](init2) {
          case (src, ShiftGraph.Single(QSU.LeftShift(_, struct, idStatus, _, repair, rot), _)) =>
            val struct2 = struct >> recFunc.ProjectKeyS(recFunc.Hole, ResultsField)

            val repair2 = repair flatMap {
              case ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
                func.ProjectKeyS(
                  AccessLeftTarget[T](accO.value(_)),
                  ResultsField)

              case ShiftTarget.AccessLeftTarget(access) =>
                AccessLeftTarget[T](access.as(_))

              case ShiftTarget.LeftTarget() =>
                scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

              case ShiftTarget.RightTarget() =>
                RightTarget[T]
            }

            val repair3 = func.ConcatMaps(
              subsetObject(AccessLeftTarget[T](accO.value(_)), OriginalField),
              func.MakeMapS(ResultsField, repair2))

            updateGraph[T, G](QSU.LeftShift[T, Symbol](src.root, struct2, idStatus, OnUndefined.Emit, repair3, rot)) map { rewritten =>
              rewritten :++ src
            }

          case (src, ShiftGraph.Multi(QSU.MultiLeftShift(_, shifts, _, repair), _)) =>
            val shifts2 = shifts map {
              case (struct, idStatus, rot) =>
                val struct2 = struct >> func.ProjectKeyS(func.Hole, ResultsField)

                (struct2, idStatus, rot)
            }

            val repair2 = repair flatMap {
              case -\/(access) =>
                func.ProjectKeyS(access.left[Int].point[FreeMapA], ResultsField)

              case \/-(idx) => idx.right[QAccess[Hole]].point[FreeMapA]
            }

            val repair3 = func.ConcatMaps(
              subsetObject(accessHoleLeftF, OriginalField),
              func.MakeMapS(ResultsField, repair2))

            updateGraph[T, G](QSU.MultiLeftShift[T, Symbol](src.root, shifts2, OnUndefined.Emit, MapFuncCore.normalized(repair3))) map { rewritten =>
              rewritten :++ src
            }
        }

        rewritten = reconstructed match {
          case reconstructed @ LeftShift(src, struct, idStatus, onUndefined, repair, rot) =>
            val origLifted = origFM >> recFunc.ProjectKeyS(repair.asRec, OriginalField)
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, ResultsField), origLifted.linearize)

            reconstructed.overwriteAtRoot(
              QSU.LeftShift(src.root, struct, idStatus, onUndefined, MapFuncCore.normalized(repair2), rot))

          case reconstructed @ MultiLeftShift(src, shifts, onUndefined, repair) =>
            val origLifted = origFM >> recFunc.ProjectKeyS(repair.asRec, OriginalField)
            val repair2 = func.ConcatMaps(func.ProjectKeyS(repair, ResultsField), origLifted.linearize)

            reconstructed.overwriteAtRoot(
              QSU.MultiLeftShift(src.root, shifts, onUndefined, MapFuncCore.normalized(repair2)))

          case reconstructed => reconstructed
        }
      } yield rewritten
    }

    def coalesceZip(
        left: List[ShiftGraph],
        leftIndices: Set[Int],
        right: List[ShiftGraph],
        rightIndices: Set[Int],
        parent: Option[QSUGraph],
        requiresDeref: Boolean): G[QSUGraph] = {

      val hasParent = parent.isDefined

      @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
      def continue(
          fakeParent: QSUGraph,
          tailL: List[ShiftGraph],
          tailR: List[ShiftGraph],
          requiresDeref: Boolean)(
          pat: Symbol => QSU[T, Symbol]): G[QSUGraph] = {

        val realParent = parent.getOrElse(fakeParent)
        val cont = updateGraph[T, G](pat(realParent.root)) map { rewritten =>
          Some(rewritten :++ realParent)
        }

        cont.flatMap(coalesceZip(tailL, leftIndices, tailR, rightIndices, _, requiresDeref))
      }

      def name(side: JoinSide) = side.fold(LeftField, RightField)

      def fixSingleStruct(struct: FreeMap, side: JoinSide): FreeMap = {
        if (hasParent && requiresDeref)
          struct >> func.ProjectKeyS(func.Hole, name(side))
        else
          struct
      }

      def fixSingleRepairForMulti(
          repair: FreeMapA[ShiftTarget[D]],
          offset: Int,
          side: JoinSide): FreeMapA[Access[D, Hole] \/ Int] = {

        repair flatMap {
          case ShiftTarget.LeftTarget() =>
            scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

          case ShiftTarget.AccessLeftTarget(access) =>
            val hole = Free.pure[MapFunc, QAccess[Hole] \/ Int](access.left[Int])

            if (hasParent && requiresDeref)
              func.ProjectKeyS(hole, name(side))
            else
              hole

          case ShiftTarget.RightTarget() =>
            Free.pure(offset.right[QAccess[Hole]])
        }
      }

      def fixSingleRepairForSingle(
          repair: FreeMapA[ShiftTarget[D]],
          side: JoinSide): FreeMapA[ShiftTarget[D]] = {

        repair flatMap {
          case target @ (ShiftTarget.LeftTarget() | ShiftTarget.RightTarget()) =>
            Free.pure(target)

          case access @ ShiftTarget.AccessLeftTarget(_) =>
            val hole = Free.pure[MapFunc, ShiftTarget[D]](access)

            if (hasParent && requiresDeref)
              func.ProjectKeyS(hole, name(side))
            else
              hole
        }
      }

      def fixMultiShifts(
          shifts: List[(FreeMap, IdStatus, QSU.Rotation)],
          side: JoinSide): List[(FreeMap, IdStatus, QSU.Rotation)] = {

        shifts map {
          case (struct, idStatus, rot) =>
            val struct2 = if (hasParent && requiresDeref)
              struct >> func.ProjectKeyS(func.Hole, name(side))
            else
              struct

            (struct2, idStatus, rot)
        }
      }

      def fixMultiRepair(
          repair: FreeMapA[QAccess[Hole] \/ Int],
          offset: Int,
          side: JoinSide): FreeMapA[QAccess[Hole] \/ Int] = {

        repair flatMap {
          case -\/(access) =>
            if (hasParent && requiresDeref)
              func.ProjectKeyS(Free.pure(access.left[Int]), name(side))
            else
              Free.pure(access.left[Int])

          case \/-(i) =>
            Free.pure((i + offset).right[QAccess[Hole]])
        }
      }

      def fixCompatible(
          repair: FreeMapA[ShiftTarget[D]],
          idStatus: IdStatus,
          resultIdStatus: IdStatus,
          side: JoinSide): FreeMapA[ShiftTarget[D]] =

        (idStatus, resultIdStatus) match {
          case (IdOnly, IncludeId) => repair flatMap {
            case ShiftTarget.LeftTarget() =>
              scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

            case ShiftTarget.RightTarget() =>
              func.ProjectIndexI(RightTarget, 0)

            case access @ ShiftTarget.AccessLeftTarget(_) =>
              val hole = Free.pure[MapFunc, ShiftTarget[D]](access)

              if (hasParent && requiresDeref)
                func.ProjectKeyS(hole, name(side))
              else
                hole
          }

          case (ExcludeId, IncludeId) => repair flatMap {
            case ShiftTarget.LeftTarget() =>
              scala.sys.error("ShiftTarget.LeftTarget in CollapseShifts")

            case ShiftTarget.RightTarget() =>
              func.ProjectIndexI(RightTarget, 1)

            case access @ ShiftTarget.AccessLeftTarget(_) =>
              val hole = Free.pure[MapFunc, ShiftTarget[D]](access)

              if (hasParent && requiresDeref)
                func.ProjectKeyS(hole, name(side))
              else
                hole
          }

          case _ => repair flatMap {
            case access @ ShiftTarget.AccessLeftTarget(_) =>
              val hole = Free.pure[MapFunc, ShiftTarget[D]](access)

              if (hasParent && requiresDeref)
                func.ProjectKeyS(hole, name(side))
              else
                hole

            case target => target.point[FreeMapA]
          }
        }

      (left, right) match {
        case
            (
              (lshift @ ShiftGraph.Single(QSU.LeftShift(fakeParent, structL, idStatusL, onUndefinedL, repairL, rotL), dimsL)) :: tailL,
              (rshift @ ShiftGraph.Single(QSU.LeftShift(_, _, idStatusR, onUndefinedR, repairR, rotR), dimsR)) :: tailR)
            if dimsL === dimsR && rotL === rotR =>

          val idStatusAdj = idStatusL |+| idStatusR

          val repairLAdj = fixCompatible(repairL, idStatusL, idStatusAdj, LeftSide)
          val repairRAdj = fixCompatible(repairR, idStatusR, idStatusAdj, RightSide)

          val ignoreStructure = repairLAdj === repairRAdj

          val repair = if (ignoreStructure) {
            repairLAdj
          } else {
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)
          }

          val structLAdj = if (hasParent && requiresDeref)
            structL >> recFunc.ProjectKeyS(recFunc.Hole, name(LeftSide))
          else
            structL

          continue(fakeParent, tailL, tailR, !ignoreStructure) { sym =>
            QSU.LeftShift[T, Symbol](
              sym, structLAdj, idStatusAdj, onUndefinedL, MapFuncCore.normalized(repair), rotL)
          }

        case
            (
              ShiftGraph.Single(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL), _) :: tailL,
              ShiftGraph.Single(QSU.LeftShift(_, structR, idStatusR, _, repairR, rotR), _) :: tailR) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, LeftSide)

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForMulti(repairR, 1, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structLAdj, idStatusL, rotL) :: (structRAdj, idStatusR, rotR) :: Nil,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case
          (
            ShiftGraph.Multi(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL), _) :: tailL,
            ShiftGraph.Single(QSU.LeftShift(_, structR, idStatusR, _, repairR, rotR), _) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForMulti(repairR, offset, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: (structRAdj, idStatusR, rotR) :: Nil,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case
          (
            ShiftGraph.Single(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL), _) :: tailL,
            ShiftGraph.Multi(QSU.MultiLeftShift(_, shiftsR, _, repairR), _) :: tailR) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForMulti(repairL, 0, LeftSide)

          val shiftsRAdj = fixMultiShifts(shiftsR, RightSide)
          val repairRAdj = fixMultiRepair(repairR, 1, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              (structLAdj, idStatusL, rotL) :: shiftsRAdj,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case
          (
            ShiftGraph.Multi(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL), _) :: tailL,
            ShiftGraph.Multi(QSU.MultiLeftShift(_, shiftsR, _, repairR), _) :: tailR) =>

          val offset = shiftsL.length

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val shiftsRAdj = fixMultiShifts(shiftsR, RightSide)
          val repairRAdj = fixMultiRepair(repairR, offset, RightSide)

          val repair =
            func.StaticMapS(
              LeftField -> repairLAdj,
              RightField -> repairRAdj)

          continue(fakeParent, tailL, tailR, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj ::: shiftsRAdj,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case
          (
            ShiftGraph.Single(QSU.LeftShift(fakeParent, structL, idStatusL, _, repairL, rotL), _) :: tailL,
            Nil) =>

          val structLAdj = fixSingleStruct(structL.linearize, LeftSide)
          val repairLAdj = fixSingleRepairForSingle(repairL, LeftSide)

          val rightSide =
            if (hasParent)
              subsetObject(AccessLeftTarget[T](accO.value(_)), RightField)
            else
              func.MakeMapS(RightField, AccessLeftTarget(accO.value(_)))

          val repair = func.ConcatMaps(rightSide, func.MakeMapS(LeftField, repairLAdj))

          continue(fakeParent, tailL, Nil, true) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              structLAdj.asRec,
              idStatusL,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair),
              rotL)
          }

        case
          (
            ShiftGraph.Multi(QSU.MultiLeftShift(fakeParent, shiftsL, _, repairL), _) :: tailL,
            Nil) =>

          val shiftsLAdj = fixMultiShifts(shiftsL, LeftSide)
          val repairLAdj = fixMultiRepair(repairL, 0, LeftSide)

          val rightSide =
            if (hasParent)
              subsetObject(AccessHole[T].map(_.left[Int]), RightField)
            else
              func.MakeMapS(RightField, AccessHole[T].map(_.left[Int]))

          val repair = func.ConcatMaps(rightSide, func.MakeMapS(LeftField, repairLAdj))

          continue(fakeParent, tailL, Nil, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsLAdj,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case
          (
            Nil,
            ShiftGraph.Single(QSU.LeftShift(fakeParent, structR, idStatusR, _, repairR, rotR), _) :: tailR) =>

          val structRAdj = fixSingleStruct(structR.linearize, RightSide)
          val repairRAdj = fixSingleRepairForSingle(repairR, RightSide)

          val leftSide =
            if (hasParent)
              subsetObject(AccessLeftTarget[T](accO.value(_)), LeftField)
            else
              func.MakeMapS(LeftField, AccessLeftTarget[T](accO.value(_)))

          val repair = func.ConcatMaps(leftSide, func.MakeMapS(RightField, repairRAdj))

          continue(fakeParent, Nil, tailR, true) { sym =>
            QSU.LeftShift[T, Symbol](
              sym,
              structRAdj.asRec,
              idStatusR,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair),
              rotR)
          }

        case
          (
            Nil,
            ShiftGraph.Multi(QSU.MultiLeftShift(fakeParent, shiftsR, _, repairR), _) :: tailR) =>

          val shiftsRAdj = fixMultiShifts(shiftsR, LeftSide)
          val repairRAdj = fixMultiRepair(repairR, 0, LeftSide)

          val leftSide =
            if (hasParent)
              subsetObject(AccessHole[T].map(_.left[Int]), LeftField)
            else
              func.MakeMapS(LeftField, AccessHole[T].map(_.left[Int]))

          val repair = func.ConcatMaps(leftSide, func.MakeMapS(RightField, repairRAdj))

          continue(fakeParent, Nil, tailR, true) { sym =>
            QSU.MultiLeftShift[T, Symbol](
              sym,
              shiftsRAdj,
              OnUndefined.Emit,
              MapFuncCore.normalized(repair))
          }

        case (Nil, Nil) =>
          // if parent is None here, it means we invoked on empty lists
          mergeIndexMaps(
            parent.getOrElse(errorImpossible),
            if (requiresDeref) func.ProjectKeyS(func.Hole, "left") else func.Hole,
            if (requiresDeref) func.ProjectKeyS(func.Hole, "right") else func.Hole)
      }
    }

    def wrapCandidate(candidateGraph: QSUGraph, index: Int): G[(QSUGraph, Set[Int])] =
      candidateGraph match {
        case (qgraph @ ConsecutiveBounded(_, shifts)) =>
          // qgraph must beLike(shifts.head)

          // shifts.head is the LAST shift in the chain
          val back = shifts.head match {
            case ShiftGraph.Single(QSU.LeftShift(parent, struct, idStatus, onUndefined, repair, rotation), _) =>
              val pat = QSU.LeftShift(
                parent.root,
                struct,
                idStatus,
                onUndefined,
                func.MakeMapS(index.toString, repair),
                rotation)

              qgraph.overwriteAtRoot(pat).point[G]

            case ShiftGraph.Multi(QSU.MultiLeftShift(parent, shifts, onUndefined, repair), _) =>
              val pat = QSU.MultiLeftShift(
                parent.root,
                shifts,
                onUndefined,
                func.MakeMapS(index.toString, repair))

              qgraph.overwriteAtRoot(pat).point[G]
          }

          back.map(g => (g, Set(index)))

        case (qgraph @ Map(parent, fm)) =>
          val back = qgraph.overwriteAtRoot(QSU.Map(parent.root, recFunc.MakeMapS(index.toString, fm))).point[G]
          back.map(g => (g, Set(index)))

        case qgraph =>
          val back = updateGraph[T, G](QSU.Map(qgraph.root, recFunc.MakeMapS(index.toString, recFunc.Hole))) map { rewritten =>
            rewritten :++ qgraph
          }

          back.map(g => (g, Set(index)))
      }

    def inlineMap(g: QSUGraph): Option[QSUGraph] = g match {
      case Map(LeftShift(src, struct, idStatus, onUndefined, repair, rot), fm) =>
        val repair2 = fm.linearize >> repair
        g.overwriteAtRoot(QSU.LeftShift(src.root, struct, idStatus, onUndefined, MapFuncCore.normalized(repair2), rot)).some

      case Map(MultiLeftShift(src, shifts, onUndefined, repair), fm) =>
        val repair2 = fm.linearize >> repair
        g.overwriteAtRoot(QSU.MultiLeftShift(src.root, shifts, onUndefined, MapFuncCore.normalized(repair2))).some
      case _ => none
    }

    def elideGuards(fm: FreeMap)(
      implicit R: Recursive.Aux[FreeMap, CoEnv[Hole, MapFunc, ?]],
      C: Corecursive.Aux[FreeMap, CoEnv[Hole, MapFunc, ?]]): FreeMap =
      R.cata[FreeMap](fm) {
        case CoEnv(\/-(MFD(MapFuncsDerived.Typecheck(result, _)))) => result
        case CoEnv(\/-(MFC(MapFuncsCore.Guard(_, _, result, _)))) => result
        case otherwise => C.embed(otherwise)
      }

    def coalesceWrapped(wrapped: List[(QSUGraph, Set[Int])]): G[(QSUGraph, Set[Int])] = {
      wrapped.tail.foldLeftM[G, (QSUGraph, Set[Int])](wrapped.head) {
        case ((graphL @ ConsecutiveBounded(_, shifts1), leftIndices), (graphR @ ConsecutiveBounded(_, shifts2), rightIndices)) =>
          val back = coalesceZip(shifts1.toList.reverse, leftIndices, shifts2.toList.reverse, rightIndices, None, false)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        case ((qgraph, leftIndices), (ConsecutiveBounded(_, shifts), rightIndices)) =>
          val back = coalesceUneven(shifts, qgraph)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        case ((ConsecutiveBounded(_, shifts), leftIndices), (qgraph, rightIndices)) =>
          val back = coalesceUneven(shifts, qgraph)

          back.map(g => (inlineMap(g).getOrElse(g), leftIndices ++ rightIndices))

        // these two graphs have to be maps on the same thing
        // if they aren't, we're in trouble
        case ((Map(parent1, left), leftIndices), (Map(parent2, right), rightIndices)) =>
          scala.Predef.assert(parent1.root === parent2.root)

          val back = mergeIndexMaps(parent1, left.linearize, right.linearize)

          back.map(g => (g, leftIndices ++ rightIndices))
      }
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def coalesceTier(tier: ShiftAssoc): G[(QSUGraph, Set[Int])] = tier match {
      case ShiftAssoc.Leaves(shifts) =>
        coalesceWrapped(shifts)

      case ShiftAssoc.Branches(shifts) =>
        shifts.traverse(coalesceTier).flatMap(coalesceWrapped)
    }

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def reassociateShifts(wrapped: List[(QSUGraph, Set[Int])], depth: Int): ShiftAssoc = {
      if (depth === 0) {
        val (maps, shifts) = wrapped partition {
          case (Map(_, _), _) => true
          case _ => false
        }

        if (maps.isEmpty)
          reassociateShifts(shifts, 1)
        else
          ShiftAssoc.Branches(List(ShiftAssoc.Leaves(maps.toList), reassociateShifts(shifts, 1)))
      } else {
        val parentGroups = wrapped groupBy {
          case (ConsecutiveBounded(_, shifts), _) =>
            // TODO this is pretty darn inefficient
            shifts.index(shifts.length - depth).map(_.bimap(_.source.root, _.source.root))

          case _ => None
        }

        // this branch is to keep the tests happy, really
        if (parentGroups.values.forall(_.lengthCompare(1) === 0)) {
          ShiftAssoc.Leaves(wrapped)
        } else {
          val outerTier = parentGroups.values.toList map { il =>
            val structGroups = il groupBy {
              case (ConsecutiveBounded(_, shifts), _) =>
                shifts.index(shifts.length - depth).map(
                  _.bimap(
                    ls => EqWrapper((elideGuards(ls.struct.linearize), ls.rot)),
                    mls => mls.shifts.map(t => EqWrapper((elideGuards(t._1), t._3))).distinct))

              case _ =>
                None
            }

            val tier = structGroups.toList map {
              case (Some(_), il) => reassociateShifts(il, depth + 1)
              case (None, il) => ShiftAssoc.Leaves(il)
            }

            ShiftAssoc.Branches(tier)
          }

          ShiftAssoc.Branches(outerTier)
        }
      }
    }

    for {
      // converts all candidates to produce final results wrapped in their relevant indices
      wrapped <- candidates.zipWithIndex traverse { case (g, i) => wrapCandidate(g, i) }
      assoc = reassociateShifts(wrapped, 0)
      (coalesced, _) <- coalesceTier(assoc)

      // we build the map node to overwrite the original autojoin (qgraph)
      back = qgraph.overwriteAtRoot(
        QSU.Map(
          coalesced.root,
          fm.flatMap(i => func.ProjectKeyS(func.Hole, i.toString)).asRec)) :++ coalesced
    } yield Some((coalesced, back))
  }

  private val ConsecutiveUnbounded = ConsecutiveLeftShifts(κ(false))

  // TODO support freemappable and Cond-able regions between shifts
  private def ConsecutiveLeftShifts(stop: QSUGraph => Boolean): Extractor = new Extractor {

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    lazy val Self = ConsecutiveLeftShifts(stop)

    def unapply(qgraph: QSUGraph): Option[(QSUGraph, NEL[ShiftGraph])] = qgraph match {
      case qgraph if stop(qgraph) =>
        None

      case
        LeftShift(
          MappableRegion.MaximalUnary(oparent @ Self(parent, inners), fm),
          struct,
          idStatus,
          onUndefined,
          repair,
          rot) =>

        val struct2 = struct >> fm.asRec

        val from = inners.head match {
          case ShiftGraph.Single(_, dims) => dims
          case ShiftGraph.Multi(_, _) => Dimensions.origin(qprov.prov.fresh())   // completely disable compatible shift collapse under MultiLeftShift (TODO do better)
        }

        val dims = computeStructProv(from, Some(inners.head), struct2.linearize)

        val repair2 = repair flatMap {
          case alt @ ShiftTarget.AccessLeftTarget(Access.Value(_)) =>
            fm.map(_ => alt: ShiftTarget[D])

          case t => Free.pure[MapFunc, ShiftTarget[D]](t)
        }

        Some((parent, ShiftGraph.Single(QSU.LeftShift[T, QSUGraph](oparent, struct2, idStatus, onUndefined, repair2, rot), dims) <:: inners))

      case LeftShift(parent, struct, idStatus, onUndefined, repair, rot) =>
        val dims = computeStructProv(QPZero, None, struct.linearize)
        Some((parent, NEL(ShiftGraph.Single(QSU.LeftShift[T, QSUGraph](parent, struct, idStatus, onUndefined, repair, rot), dims))))

      case
        MultiLeftShift(
          MappableRegion.MaximalUnary(oparent @ Self(parent, inners), fm),
          shifts,
          onUndefined,
          repair) =>

        val shifts2 = shifts map {
          case (struct, idStatus, rot) =>
            (struct >> fm, idStatus, rot)
        }

        val from = inners.head match {
          case ShiftGraph.Single(_, dims) => dims
          case ShiftGraph.Multi(_, _) => Dimensions.origin(qprov.prov.fresh())   // completely disable compatible shift collapse under MultiLeftShift (TODO do better)
        }

        val dims = shifts2 map {
          case (struct, _, _) => computeStructProv(from, Some(inners.head), struct)
        }

        val repair2 = repair flatMap {
          case l @ -\/(Access.Value(_)) =>
            fm.map(_ => l: (QAccess[Hole] \/ Int))

          case r => Free.pure[MapFunc, QAccess[Hole] \/ Int](r)
        }

        Some((parent, ShiftGraph.Multi(QSU.MultiLeftShift[T, QSUGraph](oparent, shifts2, onUndefined, repair2), dims) <:: inners))

      case MultiLeftShift(parent, shifts, onUndefined, repair) =>
        val dims = shifts map {
          case (struct, _, _) => computeStructProv(QPZero, None, struct)
        }

        Some((parent, NEL(ShiftGraph.Multi(QSU.MultiLeftShift[T, QSUGraph](parent, shifts, onUndefined, repair), dims))))

      case _ =>
        None
    }

    // TODO can we use the parent dims?
    private[this] def computeStructProv(from: QDims, wrt: Option[ShiftGraph], struct: FreeMap): QDims = wrt match {
      case Some(ShiftGraph.Single(QSU.LeftShift(_, _, idStatus, _, repair, _), _)) =>
        // we normalize the provenance to pretend that every shift is IncludeId
        // this enables comparisons between IncludeId repairs and ExcludeId/IdOnly
        val adjustment = idStatus match {
          case IdStatus.IdOnly => Some(0)
          case IdStatus.ExcludeId => Some(1)
          case IdStatus.IncludeId => None
        }

        val repair2 = adjustment map { i =>
          repair flatMap {
            case t @ ShiftTarget.RightTarget() => func.ProjectIndexI(RightTarget[T], i)
            case other => other.point[FreeMapA]
          }
        } getOrElse repair

        ApplyProvenance.computeFuncDims(struct >> repair2)(κ(from)).getOrElse(from)

      // TODO apply adjustments to multi shifts
      case Some(ShiftGraph.Multi(QSU.MultiLeftShift(_, _, _, repair), _)) =>
        ApplyProvenance.computeFuncDims(struct >> repair)(κ(from)).getOrElse(from)

      case None =>
        ApplyProvenance.computeFuncDims(struct)(κ(from)).getOrElse(from)
    }
  }

  private[this] def subsetObject[A](fm: FreeMapA[A], field: String): FreeMapA[A] =
    func.MakeMapS(field, func.ProjectKeyS(fm, field))

  // each List within a given ShiftAssoc may be coalesced in any order, producing an equivalent result
  private sealed trait ShiftAssoc extends Product with Serializable {
    def explore: String
  }

  @SuppressWarnings(Array("org.wartremover.warts.LeakingSealed"))
  private object ShiftAssoc {

    case class Leaves(tier: List[(QSUGraph, Set[Int])]) extends ShiftAssoc {
      def explore = tier.map(_._1.shows).mkString("Leaves(\n", ",\n", ")")
    }

    case class Branches(tier: List[ShiftAssoc]) extends ShiftAssoc {
      def explore = tier.map(_.explore).mkString("Branches(", ", ", ")")
    }
  }

  // FML
  private final class EqWrapper[A: Equal](private val inner: A) {

    @SuppressWarnings(
      Array(
        "org.wartremover.warts.IsInstanceOf",
        "org.wartremover.warts.AsInstanceOf"))
    override def equals(other: Any): Boolean = {
      if (other.isInstanceOf[EqWrapper[_]])
        other.asInstanceOf[EqWrapper[A]].inner === inner
      else
        false
    }

    override def hashCode = 0
  }

  private object EqWrapper {
    def apply[A: Equal](a: A): EqWrapper[A] = new EqWrapper(a)
  }

  private trait Extractor {
    def unapply(qgraph: QSUGraph) : Option[(QSUGraph, NEL[ShiftGraph])]
  }
}

object CollapseShifts {
  def apply[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: CollapseShifts[T] =
    new CollapseShifts[T]
}
