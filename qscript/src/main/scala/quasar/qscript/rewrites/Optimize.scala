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
import quasar.fp.{liftFG, Injectable}
import quasar.qscript._
import quasar.qscript.MapFuncCore._
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._

import scalaz.{Const, Functor}
import scalaz.Scalaz._ // apply-traverse syntax conflict

final class Optimize[T[_[_]]: BirecursiveT: EqualT] extends TTypes[T] {

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

    // LeftShift(ShiftedRead(_, ExcludeId), Hole, _, Map, Omit, f(RightSide))
    case qc @ LeftShift(Embed(src), struct, shiftStatus, ShiftType.Map, OnUndefined.Omit, repair)
        if struct === HoleR => {

      val key = ShiftKey("shifted")

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

      val rewritten: Option[F[T[F]]] = (mfOpt |@| srOpt) { case (mf, sr) =>
        val src: T[F] = ER.inj(Const[ExtraShiftedRead[A], T[F]](
          ExtraShiftedRead[A](sr.path, shiftStatus, key))).embed

        QC.inj(Map(src, mf.asRec))
      }

      rewritten.getOrElse(QC.inj(qc))
    }

    case qc => QC.inj(qc)
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
