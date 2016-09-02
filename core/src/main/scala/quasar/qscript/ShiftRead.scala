/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.Predef._
import quasar.fp._
import quasar.qscript.MapFuncs._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._

/** This optional transformation changes the semantics of [[Read]]. The default
  * semantics return a single value, whereas the transformed version has an
  * implied [[LeftShift]] and therefore returns a set of values, which more
  * closely matches the way many data stores behave.
  */
trait ShiftRead[T[_[_]], H[_]] {
  def shiftRead[F[_]: Functor, G[_]: Functor]
    (GtoF: PrismNT[G, F], H: H ~> F)
    (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
      : H[T[G]] => G[T[G]]
}

object ShiftRead extends ShiftReadInstances {
  def apply[T[_[_]], H[_]](implicit ev: ShiftRead[T, H]) = ev

  def applyToBranch[T[_[_]]: Recursive: Corecursive](branch: FreeQS[T]):
      FreeQS[T] =
    freeTransCata(branch)(
      liftCo(ShiftRead[T, QScriptTotal[T, ?]].shiftRead(
        coenvPrism[QScriptTotal[T, ?], Hole],
        NaturalTransformation.refl)))

  implicit def sourcedPathable[T[_[_]]: Recursive: Corecursive]:
      ShiftRead[T, SourcedPathable[T, ?]] =
    new ShiftRead[T, SourcedPathable[T, ?]] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: SourcedPathable[T, ?] ~> F)
        (implicit
          R: Const[Read, ?] :<: F,
          QC: QScriptCore[T, ?] :<: F)
          : SourcedPathable[T, T[G]] => G[T[G]] = {
        case x @ LeftShift(src, struct, repair) =>
          (GtoF.get(src.project) >>= R.prj).fold(
            GtoF.reverseGet(H(x)))(
            read => (struct.resume.toOption >> (
              repair.traverseM[Option, Hole] {
                case LeftSide => None
                case RightSide => HoleF.some
              } ∘ (rep => GtoF.reverseGet(QC.inj(Map(GtoF.reverseGet(R.inj(read)).embed, rep)))))).getOrElse(
              GtoF.reverseGet(H(LeftShift(GtoF.reverseGet(QC.inj(reduceSrc(src))).embed, struct, repair)))))
        case Union(src, lb, rb) =>
          GtoF.reverseGet(H(Union(reduceRead(GtoF)(src), applyToBranch(lb), applyToBranch(rb))))
      }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive]:
      ShiftRead[T, QScriptCore[T, ?]] =
    new ShiftRead[T, QScriptCore[T, ?]] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: QScriptCore[T, ?] ~> F)
        (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
          : QScriptCore[T, T[G]] => G[T[G]] = {
        case Drop(src, lb, rb) =>
          GtoF.reverseGet(H(Drop(
            reduceRead(GtoF)(src),
            applyToBranch(lb),
            applyToBranch(rb))))
        case Take(src, lb, rb) =>
          GtoF.reverseGet(H(Take(
            reduceRead(GtoF)(src),
            applyToBranch(lb),
            applyToBranch(rb))))
        case x => GtoF.reverseGet(H(x.map(reduceRead(GtoF))))
      }
    }

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive]:
      ShiftRead[T, ThetaJoin[T, ?]] =
    new ShiftRead[T, ThetaJoin[T, ?]] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: ThetaJoin[T, ?] ~> F)
        (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
          : ThetaJoin[T, T[G]] => G[T[G]] =
        tj => GtoF.reverseGet(H(ThetaJoin(
          reduceRead(GtoF)(tj.src),
          applyToBranch(tj.lBranch),
          applyToBranch(tj.rBranch),
          tj.on,
          tj.f,
          tj.combine)))
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive]:
      ShiftRead[T, EquiJoin[T, ?]] =
    new ShiftRead[T, EquiJoin[T, ?]] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: EquiJoin[T, ?] ~> F)
        (implicit
          R: Const[Read, ?] :<: F,
          QC: QScriptCore[T, ?] :<: F)
          : EquiJoin[T, T[G]] => G[T[G]] =
        ej => GtoF.reverseGet(H(EquiJoin(
          reduceRead(GtoF)(ej.src),
          applyToBranch(ej.lBranch),
          applyToBranch(ej.rBranch),
          ej.lKey,
          ej.rKey,
          ej.f,
          ej.combine)))
    }

  implicit def coproduct[T[_[_]]: Recursive: Corecursive, H[_], I[_]]
    (implicit HS: ShiftRead[T, H], IS: ShiftRead[T, I]):
      ShiftRead[T, Coproduct[H, I, ?]] =
    new ShiftRead[T, Coproduct[H, I, ?]] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: Coproduct[H, I, ?] ~> F)
        (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
          : Coproduct[H, I, T[G]] => G[T[G]] =
        _.run.fold(
          HS.shiftRead[F, G](GtoF, H.compose(Inject[H, Coproduct[H, I, ?]])),
          IS.shiftRead[F, G](GtoF, H.compose(Inject[I, Coproduct[H, I, ?]])))
    }
}

abstract class ShiftReadInstances {
  def reduceSrc[T[_[_]]: Corecursive, A](src: A): QScriptCore[T, A] =
    Reduce(
      src,
      NullLit(),
      List(ReduceFuncs.UnshiftMap(Free.roll(ExtractId(HoleF)), HoleF)),
      Free.point(ReduceIndex(0)))

  def reduceRead[T[_[_]]: Recursive: Corecursive, F[_], G[_]: Functor]
    (GtoF: PrismNT[G, F])
    (src: T[G])
    (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
      : T[G] =
    (GtoF.get(src.project) >>= R.prj).fold(
      src)(
      κ(GtoF.reverseGet(QC.inj(reduceSrc(src))).embed))

  implicit def inject[T[_[_]]: Recursive: Corecursive, H[_]: Traverse]:
      ShiftRead[T, H] =
    new ShiftRead[T, H] {
      def shiftRead[F[_]: Functor, G[_]: Functor]
        (GtoF: PrismNT[G, F], H: H ~> F)
        (implicit R: Const[Read, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
          : H[T[G]] => G[T[G]] =
        i => GtoF.reverseGet(H(i.map(reduceRead(GtoF))))
    }
}
