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

import slamdata.Predef._

import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.qscript.qsu.QSUGraph.Extractors._
import quasar.qscript.{
  construction,
  Hole,
  SrcHole
}
import quasar.qscript.provenance.Dimensions
import quasar.qscript.qsu.{QScriptUniform => QSU}
import QSU.Rotation
import quasar.qscript.qsu.ApplyProvenance.AuthenticatedQSU
import quasar.contrib.scalaz._

import matryoshka.{Hole => _, _}
import scalaz._
import scalaz.Scalaz._
import WriterT.writerTMonadListen

final class ExpandShifts[T[_[_]]: BirecursiveT: EqualT] extends QSUTTypes[T] {
  val func = construction.Func[T]
  val hole: Hole = SrcHole

  private val prov = new QProv[T]
  private type P = prov.P
  private type DimsT[F[_], A] = WriterT[F, Dims, A]
  private type Dims = List[(Symbol, Dimensions[P])]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](aqsu: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] = {
    type G[A] = DimsT[StateT[F, RevIdx, ?], A]

    for {
      pair <- aqsu.graph.rewriteM[G](expandShifts[G](aqsu.dims)).run.eval(aqsu.graph.generateRevIndex)
      (dims, graph) = pair
    } yield AuthenticatedQSU(graph, dims.foldLeft(aqsu.dims)((dims, dim) => dims + dim))
  }

  val originalKey = "original"

  def rotationsCompatible(rotation1: Rotation, rotation2: Rotation): Boolean = rotation1 match {
    case Rotation.FlattenArray | Rotation.ShiftArray =>
      rotation2 === Rotation.FlattenArray || rotation2 === Rotation.ShiftArray
    case Rotation.FlattenMap | Rotation.ShiftMap =>
      rotation2 === Rotation.FlattenMap || rotation2 === Rotation.ShiftMap
  }

  def expandShifts[
    G[_]: Monad: NameGenerator: MonadState_[?[_], RevIdx]: PlannerErrorME: MonadTell_[?[_], Dims]
    ](dims: QSUDims[T])
      : PartialFunction[QSUGraph, G[QSUGraph]] = {
    case mls@MultiLeftShift(source, shifts, repair) =>
      val mapper = repair.flatMap {
        case -\/(_) => func.ProjectKeyS(func.Hole, originalKey)
        case \/-(i) => func.ProjectKeyS(func.Hole, i.toString)
      }
      val sortedShifts = IList.fromList(shifts).sortBy(_._3).toList
      val shiftedG = sortedShifts match {
        case (struct, idStatus, rotation) :: ss =>
          val firstRepair: FreeMapA[QScriptUniform.ShiftTarget] =
            func.ConcatMaps(
              func.MakeMapS("original", func.AccessLeftTarget(Access.valueHole(_))),
              func.MakeMapS("0", func.RightTarget)
            )
          val firstShiftPat: QScriptUniform[Symbol] =
            QSU.LeftShift[T, Symbol](source.root, struct, idStatus, firstRepair, rotation)
          for {
            firstShift <- QSUGraph.withName[T, G](firstShiftPat)
            firstShiftDim <- ApplyProvenance.computeProvenanceƒ[T, G].apply(
              QSUGraph.QSUPattern(firstShift.root, firstShiftPat.map(s => (s, dims(s)))))
            _ <- MonadTell_[G, Dims].tell(firstShiftDim :: Nil)
            shiftAndDimAndRotation <- ss.zipWithIndex.foldLeftM[G, (QSUGraph, Dimensions[P], Rotation)]((firstShift :++ mls, firstShiftDim._2, rotation)) {
              case ((shiftAbove, shiftAboveDim, rotationAbove), ((newStruct, newIdStatus, newRotation), idx)) =>
                val repair = func.ConcatMaps(func.AccessLeftTarget(Access.valueHole(_)), func.MakeMapS((idx + 1).toString, func.RightTarget))
                val struct = newStruct >> func.ProjectKeyS(func.Hole, originalKey)
                val newShiftPat =
                  QSU.LeftShift[T, Symbol](shiftAbove.root, struct, newIdStatus, repair, newRotation)
                for {
                  newShift <- QSUGraph.withName[T, G](newShiftPat)
                  newShiftDim <- ApplyProvenance.computeProvenanceƒ[T, G].apply(
                    QSUGraph.QSUPattern(newShift.root, (newShiftPat: QScriptUniform[Symbol]).map(s => (s, shiftAboveDim)))
                  )
                  identityCondition =
                  if (rotationsCompatible(rotationAbove, newRotation))
                    func.Cond(
                      func.Eq(
                        func.AccessLeftTarget(Access.identityHole(shiftAbove.root, _)),
                        func.AccessLeftTarget(Access.identityHole(newShift.root, _))),
                      repair,
                      func.Undefined
                    )
                  else repair
                  newShiftNewRepairPat =
                    newShiftPat.copy(repair = identityCondition)
                  newShiftNewRepair = newShift.overwriteAtRoot(newShiftNewRepairPat)
                  _ <- MonadTell_[G, Dims].tell((newShiftNewRepair.root -> newShiftDim._2) :: Nil)
                } yield (newShiftNewRepair :++ shiftAbove, newShiftDim._2, newRotation)
            }
            (nestedShifts, _, _) = shiftAndDimAndRotation
          } yield nestedShifts
        case Nil => source.pure[G]
      }
      for {
        shifted <- shiftedG
        map = QSU.Map[T, Symbol](shifted.root, mapper)
      } yield mls.overwriteAtRoot(map) :++ shifted
  }
}

object ExpandShifts {
  def apply[
      T[_[_]]: BirecursiveT: EqualT,
      F[_]: Monad: NameGenerator: PlannerErrorME](
      qgraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    new ExpandShifts[T].apply[F](qgraph)
}
