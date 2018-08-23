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

import slamdata.Predef._

import quasar.common.effect.NameGenerator
import quasar.contrib.scalaz._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp.ski.κ
import quasar.qscript.{
  IdStatus,
  MonadPlannerErr,
  OnUndefined,
  construction
}
import quasar.qscript.RecFreeS._
import quasar.qsu.{QScriptUniform => QSU}, QSU.Rotation
import quasar.qsu.QSUGraph.Extractors._
import quasar.qsu.ApplyProvenance.AuthenticatedQSU
import quasar.contrib.scalaz._

import matryoshka.{Hole => _, _}
import monocle.syntax.fields._
import scalaz._
import scalaz.Scalaz._
import StateT.stateTMonadState

final class ExpandShifts[T[_[_]]: BirecursiveT: EqualT: ShowT] extends QSUTTypes[T] {

  private val prov = new QProv[T]
  private val func = construction.Func[T]
  private val json = Fixed[T[EJson]]
  private type P = prov.P
  private type QAuthS[F[_]] = MonadState_[F, QAuth]
  private type MonadCI[F[_]] = MonadState_[F, CompatInfo]
  private type CompatInfo = IList[(Symbol, Rotation, QDims)]
  private type S = (QAuth, RevIdx, CompatInfo)

  private implicit def qauthState[F[_]: Monad]: MonadState_[StateT[F, S, ?], QAuth] =
    MonadState_.zoom[StateT[F, S, ?]](_1[S, QAuth])

  private implicit def revIdxState[F[_]: Monad]: MonadState_[StateT[F, S, ?], RevIdx] =
    MonadState_.zoom[StateT[F, S, ?]](_2[S, RevIdx])

  private implicit def ciState[F[_]: Monad]: MonadState_[StateT[F, S, ?], CompatInfo] =
    MonadState_.zoom[StateT[F, S, ?]](_3[S, CompatInfo])

  import prov.prov.implicits._

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def apply[F[_]: Monad: NameGenerator: MonadPlannerErr](aqsu: AuthenticatedQSU[T]): F[AuthenticatedQSU[T]] =
    aqsu.graph.rewriteM[StateT[F, S, ?]](expandShifts[StateT[F, S, ?]])
      .run((aqsu.auth, aqsu.graph.generateRevIndex, IList()))
      .map { case ((auth, _, _), graph) => AuthenticatedQSU(graph, auth) }

  @SuppressWarnings(Array("org.wartremover.warts.TraversableOps"))
  def expandShifts[G[_]: Monad: NameGenerator: RevIdxM: MonadPlannerErr: QAuthS: MonadCI]
      : PartialFunction[QSUGraph, G[QSUGraph]] = {
    case mls @ MultiLeftShift(source, shifts, _, repair) =>
      shifts.toNel.fold(source.pure[G]) { shifts1 =>
        val indexed = shifts.zipWithIndex

        val shiftedG = for {
          _ <- MonadState_[G, CompatInfo].put(IList())
          headShift <- buildShift[G](source.root, source, indexed.head)
          expandedShifts <- indexed.tail.foldLeftM(headShift)(buildShift[G](source.root, _, _))
        } yield expandedShifts

        val mapper = repair.flatMap {
          case -\/(_) => PrjOriginal
          case \/-(i) => func.ProjectKeyS(func.Hole, i.toString)
        }

        shiftedG map { shifted =>
          mls.overwriteAtRoot(QSU.Map[T, Symbol](shifted.root, mapper.asRec)) :++ shifted
        }
      }
  }

  ////

  private val OriginalKey = "original"
  private val NamePrefix = "esh"
  private val SrcVal = AccessLeftTarget[T](Access.value(_))
  private val PrjOriginal = func.ProjectKeyS(func.Hole, OriginalKey)

  private def buildShift[G[_]: Monad: NameGenerator: RevIdxM: MonadPlannerErr: QAuthS: MonadCI](
      commonRoot: Symbol,
      src: QSUGraph,
      shift: ((FreeMap, IdStatus, Rotation), Int))
      : G[QSUGraph] = {

    val ((struct, status, rotation), idx) = shift

    val repair =
      func.ConcatMaps(
        if (idx === 0) func.MakeMapS(OriginalKey, SrcVal) else SrcVal,
        func.MakeMapS(idx.toString, RightTarget))

    val adjustedStruct =
      if (idx === 0) struct else struct >> PrjOriginal

    // Use `src.root` to ensure we get a fresh name, if we use `commonRoot` it will
    // find the original shift in the reverse index.
    //
    // Eventually, we'll probably want the original shift as, if its provenance was
    // correct we'd just use it. As-is, it contains existentials due to uncollapsed
    // mappable regions prior to provenance computation.
    val tempShiftPat =
      QSU.LeftShift[T, Symbol](
        src.root, struct.asRec, status, OnUndefined.Emit, RightTarget, rotation)

    def dimsOf(sym: Symbol): G[QDims] =
      MonadState_[G, QAuth].get >>= (_.lookupDimsE[G](sym))

    for {
      commonSrcDims <- dimsOf(commonRoot)

      structDim =
        ApplyProvenance.computeFuncDims(struct)(κ(commonSrcDims))
          .getOrElse(commonSrcDims)

      compatInfo <- MonadState_[G, CompatInfo].get

      sameFocus = compatInfo findLeft {
        case (_, rot, qds) => (rotation ≟ rot) && (structDim ≟ qds)
      }

      tempShift <- QSUGraph.withName[T, G](NamePrefix)(tempShiftPat)
      commonShift = tempShift.overwriteAtRoot(tempShiftPat.copy(source = commonRoot))

      joinRepair = sameFocus map {
        case (sym, _, _) =>
          func.Cond(
            func.Or(
              func.Eq(
                AccessLeftTarget[T](Access.id(IdAccess.identity(sym), _)),
                AccessLeftTarget[T](Access.id(IdAccess.identity(commonShift.root), _))),
              func.IfUndefined(
                AccessLeftTarget[T](Access.id(IdAccess.identity(commonShift.root), _)),
                func.Constant(json.bool(true)))),
            repair,
            func.Undefined)
      }

      srcDims <- dimsOf(src.root)

      // If another shift matched, use its dimensions, otherwise compute
      // provenance for this shift on the common source.
      newDims <- sameFocus match {
        case Some((sym, _, _)) => dimsOf(sym)
        case None => ApplyProvenance.computeDims[T, G](commonShift)
      }

      joinDims =
        if (idx === 0) newDims else prov.join(newDims, srcDims)

      _ <- MonadState_[G, QAuth].modify(_.addDims(commonShift.root, joinDims))

      newShift = commonShift overwriteAtRoot {
        tempShiftPat.copy(
          struct = adjustedStruct.asRec,
          repair = joinRepair getOrElse repair)
      }

      _ <- MonadState_[G, CompatInfo].put((newShift.root, rotation, structDim) :: compatInfo)
    } yield newShift :++ src
  }
}

object ExpandShifts {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT,
      F[_]: Monad: NameGenerator: MonadPlannerErr](
      qgraph: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    new ExpandShifts[T].apply[F](qgraph)
}
