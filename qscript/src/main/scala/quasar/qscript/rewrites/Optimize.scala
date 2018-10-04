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

package quasar.qscript.rewrites

import slamdata.Predef.{Map => _, _}
import quasar.contrib.iota._
import quasar.ejson
import quasar.fp.{liftFG, Injectable}
import quasar.qscript._
import quasar.qscript.MapFuncCore._
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns.{ginterpretM, CoEnv}

import scalaz.{\/, -\/, \/-, Const, Functor}
import scalaz.Scalaz._ // apply-traverse syntax conflict

final class Optimize[T[_[_]]: BirecursiveT: EqualT] extends TTypes[T] {

  // We perform this rewrite before `extraShift` because sometimes
  // a no-op Map is added in the transformation from `QScriptEducated`,
  // which interferes with the pattern matching in `extraShift`.
  def elideNoopMap[F[a] <: ACopK[a]: Functor](implicit QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {
    case Map(Embed(src), mf) if mf === HoleR => src
    case qc => QC.inj(qc)
  }

  def extraShift[F[a] <: ACopK[a]: Functor, A](
      implicit
        ER: Const[ExtraShiftedRead[A], ?] :<<: F,
        SR: Const[ShiftedRead[A], ?] :<<: F,
        QC: QScriptCore :<<: F)
      : QScriptCore[T[F]] => F[T[F]] = {

    // LeftShift(ShiftedRead(_, ExcludeId), /foo/bar/, _, _, Omit, f(RightSide))
    case qc @ LeftShift(Embed(src), struct, shiftStatus, shiftType, OnUndefined.Omit, repair) => {

      val key: ShiftKey = ShiftKey(ShiftedKey)

      val pathOpt: Option[ShiftPath] = findPath(struct.linearize)

      val mfOpt: Option[FreeMap] =
        repair.traverseM[Option, Hole] {
          case LeftSide => None  // repair must not contain LeftSide
          case RightSide => construction.Func[T].ProjectKeyS(HoleF, key.key).some
        }

      val srOpt: Option[ShiftedRead[A]] = SR.prj(src) flatMap { srConst =>
        val sr = srConst.getConst

        sr.idStatus match {
          case ExcludeId => sr.some  // only rewrite if ShiftedRead has ExcludeId
          case _ => None
        }
      }

      val rewritten: Option[F[T[F]]] = (mfOpt |@| srOpt |@| pathOpt) {
        case (mf, sr, path) =>
          val src: T[F] = ER.inj(Const[ExtraShiftedRead[A], T[F]](
            ExtraShiftedRead[A](sr.path, path, shiftStatus, shiftType, key))).embed

          QC.inj(Map(src, mf.asRec))
      }

      rewritten.getOrElse(QC.inj(qc))
    }

    case qc => QC.inj(qc)
  }

  /* Matches on a `FreeMap` of the form
   * ```
   * ProjectKey(ProjectKey(...(ProjectKey(Hole, Const(Str))), Const(Str)), Const(Str))
   * ```
   * returning the `ShiftPath` when that form is matched.
   */
  def findPath(fm: FreeMap): Option[ShiftPath] = {
    import MapFuncsCore.{Constant, ProjectKey}

    // A `FreeMap` effectively has two leaf types: `Constant` and `Hole`.
    // When we hit a `Constant` we need to keep searching even though
    // we haven't found a path to shift, and so we encode this as `Unit`.
    type Acc = Unit \/ ShiftPath

    type CoFreeMap = CoEnv[Hole, MapFunc, (FreeMap, Acc)]

    def transformHole(hole: Hole): Option[Acc] =
      ShiftPath(List[String]()).right.some

    def transformFunc(func: MapFunc[(FreeMap, Acc)]): Option[Acc] =
      func match {
        case MFC(Constant(Embed(EC(ejson.Str(_))))) =>
          ().left.some // this is awkward but we have to keep going

        // the source of a `ProjectKey` must have encoded as a `ShiftPath`
        case MFC(ProjectKey((_, \/-(srcPath)), (ExtractFunc(Constant(Embed(EC(ejson.Str(str))))), _))) =>
          ShiftPath(str :: srcPath.path).right.some

        case _ => none
      }

    def transform: CoFreeMap => Option[Acc] =
      ginterpretM[(FreeMap, ?), Option, MapFunc, Hole, Acc](
        transformHole(_),
        transformFunc(_))

    val transformed: Option[Acc] =
      fm.paraM[Option, Acc](transform)

    transformed flatMap {
      case -\/(_) => none
      case \/-(path) => ShiftPath(path.path.reverse).some
    }
  }
}

object Optimize {
  def apply[T[_[_]]: BirecursiveT: EqualT, F[a] <: ACopK[a]: Functor, G[a] <: ACopK[a], A](
      implicit GF: Injectable[G, F],
               ESRF: Const[ExtraShiftedRead[A], ?] :<<: F,
               SRF: Const[ShiftedRead[A], ?] :<<: F,
               QCF: QScriptCore[T, ?] :<<: F,
               QCG: QScriptCore[T, ?] :<<: G)
      : G[T[F]] => F[T[F]] = {

    val opt = new Optimize[T]

    val rewrite1: G[T[F]] => F[T[F]] =
      gtf => QCG.prj(gtf).fold(GF.inject(gtf))(opt.elideNoopMap[F])

    val rewrite2: F[T[F]] => F[T[F]] =
      liftFG[QScriptCore[T, ?], F, T[F]](opt.extraShift[F, A])

    rewrite1 andThen rewrite2
  }
}
