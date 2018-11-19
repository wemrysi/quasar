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
import quasar.RenderTreeT
import quasar.IdStatus, IdStatus.{ExcludeId, IdOnly, IncludeId}
import quasar.api.resource.ResourcePath
import quasar.contrib.matryoshka._
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript._
import quasar.qscript.RecFreeS._
import quasar.qscript.MapFuncCore._
import quasar.qscript.MapFuncsCore._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _},
  BijectionT._,
  Leibniz._,
  Scalaz._

class Rewrite[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {

  def rewriteShift(idStatus: IdStatus, repair: JoinFunc)
      : Option[(IdStatus, JoinFunc)] =
    (idStatus === IncludeId).option[Option[(IdStatus, JoinFunc)]] {
      def makeRef(idx: Int): JoinFunc =
        Free.roll[MapFunc, JoinSide](MFC(ProjectIndex(RightSideF, IntLit(idx))))

      val zeroRef: JoinFunc = makeRef(0)
      val oneRef: JoinFunc = makeRef(1)
      val rightCount: Int = repair.elgotPara[Int](count(RightSideF))

      if (repair.elgotPara[Int](count(oneRef)) === rightCount)
        // all `RightSide` access is through `oneRef`
        (ExcludeId, repair.transApoT(substitute[JoinFunc](oneRef, RightSideF))).some
      else if (repair.elgotPara[Int](count(zeroRef)) ≟ rightCount)
        // all `RightSide` access is through `zeroRef`
        (IdOnly, repair.transApoT(substitute[JoinFunc](zeroRef, RightSideF))).some
      else
        None
    }.join

  def normTJ[G[a] <: ACopK[a]: Traverse]
    (implicit QC: QScriptCore :<<: G,
              TJ: ThetaJoin :<<: G,
              SD: Const[Read[ResourcePath], ?] :<<: G,
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[G] => T[G] =
        _.cata[T[G]](
          normalizeTJ[G] >>>
          repeatedly(N.normalizeF(_: G[T[G]])) >>>
          (_.embed))

  def simplifyJoinOnNorm[G[a] <: ACopK[a]: Traverse, H[_]: Functor]
    (implicit QC: QScriptCore :<<: G,
              TJ: ThetaJoin :<<: G,
              SF: Const[Read[ResourcePath], ?] :<<: G,
              J: SimplifyJoin.Aux[T, G, H],
              C: Coalesce.Aux[T, G, G],
              N: Normalizable[G])
      : T[G] => T[H] =
    normTJ[G].apply(_).transCata[T[H]](J.simplifyJoin[J.G](idPrism.reverseGet))

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, HoleF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncsCore
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  def elideNopQC[F[_]: Functor]: QScriptCore[T[F]] => Option[F[T[F]]] = {
    case Filter(Embed(src), RecBoolLit(true)) => some(src)
    case Map(Embed(src), mf) if mf ≟ HoleR    => some(src)
    case _                                    => none
  }

  def compactLeftShift[F[_]: Functor]
      (QCToF: PrismNT[F, QScriptCore])
      : QScriptCore[T[F]] => Option[F[T[F]]] = {
    case qs @ LeftShift(Embed(src), struct, ExcludeId, shiftType, OnUndefined.Emit, joinFunc) =>
      (QCToF.get(src), struct.resume) match {
        // LeftShift(Map(_, MakeArray(_)), Hole, ExcludeId, _)
        case (Some(Map(innerSrc, fm)), \/-(SrcHole)) =>
          fm.resume match {
            case -\/(Suspend(MFC(MakeArray(value)))) =>
              QCToF(Map(innerSrc, (joinFunc.asRec >>= {
                case LeftSide => fm
                case RightSide => value
              }))).some
            case _ => None
          }
        // LeftShift(_, MakeArray(_), ExcludeId, _)
        case (_, -\/(Suspend(MFC(MakeArray(value))))) =>
          QCToF(Map(src.embed, (joinFunc.asRec >>= {
            case LeftSide => HoleR
            case RightSide => value
          }))).some
        case (_, _) => None
      }
    case qs => None
  }

  val compactQC = λ[QScriptCore ~> (Option ∘ QScriptCore)#λ] {
    case LeftShift(src, struct, id, stpe, undef, repair) =>
      rewriteShift(id, repair) ∘ (xy => LeftShift(src, struct, xy._1, stpe, undef, xy._2))

    case _ => None
  }

  private def applyNormalizations[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor](
    prism: PrismNT[G, F],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C: Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[T[G]] => G[T[G]] = {

    val qcPrism = PrismNT.injectCopK[QScriptCore, F] compose prism

    ftf => repeatedly[G[T[G]]](applyTransforms[G[T[G]]](
      liftFFTrans[F, G, T[G]](prism)(Normalizable[F].normalizeF(_: F[T[G]])),
      liftFFTrans[QScriptCore, G, T[G]](qcPrism)(compactQC(_: QScriptCore[T[G]])),
      liftFGTrans[QScriptCore, G, T[G]](qcPrism)(compactLeftShift[G](qcPrism)),
      liftFFTrans[F, G, T[G]](prism)(C.coalesceQC[G](prism)),
      liftFGTrans[F, G, T[G]](prism)(normalizeJoins),
      liftFGTrans[QScriptCore, G, T[G]](qcPrism)(elideNopQC[G])
    ))(prism(ftf))
  }

  private def normalizeWithBijection[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F],
    normalizeJoins: F[T[G]] => Option[G[T[G]]])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F):
      F[A] => G[A] =
    fa => applyNormalizations[F, G](prism, normalizeJoins)
      .apply(fa ∘ bij.toK.run) ∘ bij.fromK.run

  private def normalizeEJBijection[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[A] => G[A] = {

    val normEJ: G[T[G]] => Option[G[T[G]]] =
      liftFFTrans[F, G, T[G]](prism)(C.coalesceEJ[G](prism.get))

    normalizeWithBijection[F, G, A](bij)(prism, normEJ compose (prism apply _))
  }

  def normalizeEJ[F[a] <: ACopK[a]: Functor: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[T[F]] => F[T[F]] =
    normalizeEJBijection[F, F, T[F]](bijectionId)(idPrism)

  def normalizeEJCoEnv[F[a] <: ACopK[a]: Functor: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             EJ: EquiJoin :<<: F):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeEJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism)

  private def normalizeTJBijection[F[a] <: ACopK[a]: Functor: Normalizable, G[_]: Functor, A](
    bij: Bijection[A, T[G]])(
    prism: PrismNT[G, F])(
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[A] => G[A] = {

    val normTJ = applyTransforms(
      liftFFTrans[F, G, T[G]](prism)(C.coalesceTJ[G](prism.get)))

    normalizeWithBijection[F, G, A](bij)(prism, normTJ compose (prism apply _))
  }

  def normalizeTJ[F[a] <: ACopK[a]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[T[F]] => F[T[F]] =
    normalizeTJBijection[F, F, T[F]](bijectionId)(idPrism)

  def normalizeTJCoEnv[F[a] <: ACopK[a]: Traverse: Normalizable](
    implicit C:  Coalesce.Aux[T, F, F],
             QC: QScriptCore :<<: F,
             TJ: ThetaJoin :<<: F):
      F[Free[F, Hole]] => CoEnv[Hole, F, Free[F, Hole]] =
    normalizeTJBijection[F, CoEnv[Hole, F, ?], Free[F, Hole]](coenvBijection)(coenvPrism)
}
