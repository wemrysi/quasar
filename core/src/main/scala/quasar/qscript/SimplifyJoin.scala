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

import matryoshka._
import scalaz._, Scalaz._

/** Replaces [[ThetaJoin]] with [[EquiJoin]], which is often more feasible for
  * connectors to implement. It potentially adds a [[Filter]] iff there are
  * conditions in the [[ThetaJoin]] that can not be handled by an [[EquiJoin]].
  */
trait SimplifyJoin[F[_]] {
  type IT[F[_]]
  type G[A]

  def simplifyJoin[H[_]: Functor](GtoH: G ~> H): F[IT[H]] => H[IT[H]]
}

object SimplifyJoin extends SimplifyJoinInstances {
  type Aux[T[_[_]], F[_], H[_]] = SimplifyJoin[F] {
    type IT[F[_]] = T[F]
    type G[A] = H[A]
  }

  def apply[T[_[_]], F[_], G[_]](implicit ev: SimplifyJoin.Aux[T, F, G]) = ev

  def applyToBranch[T[_[_]]: Recursive: Corecursive](branch: FreeQS[T])
      : FreeQS[T] =
    freeTransCata(branch)(liftCo(SimplifyJoin[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].simplifyJoin(coenvPrism.reverseGet)))

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive: EqualT: ShowT, F[_]]
    (implicit EJ: EquiJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
      : SimplifyJoin.Aux[T, ThetaJoin[T, ?], F] =
    new SimplifyJoin[ThetaJoin[T, ?]] {
      import MapFunc._
      import MapFuncs._

      type IT[F[_]] = T[F]
      type G[A] = F[A]

      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): ThetaJoin[T, T[H]] => H[T[H]] =
        tj => {
          // TODO: This can potentially rewrite conditions to try to get left
          //       and right references on distinct sides.
          def alignCondition(l: JoinFunc[T], r: JoinFunc[T]): Option[EquiJoinKey[T]] =
            if (l.element(LeftSide) && r.element(RightSide) &&
              !l.element(RightSide) && !r.element(LeftSide))
              EquiJoinKey(l.as[Hole](SrcHole), r.as[Hole](SrcHole)).some
            else if (l.element(RightSide) && r.element(LeftSide) &&
              !l.element(LeftSide) && !r.element(RightSide))
              EquiJoinKey(r.as[Hole](SrcHole), l.as[Hole](SrcHole)).some
            else None

          def separateConditions(fm: JoinFunc[T]): SimplifiedJoinCondition[T] =
            fm.resume match {
              case -\/(And(a, b)) =>
                val (fir, sec) = (separateConditions(a), separateConditions(b))
                SimplifiedJoinCondition(
                  fir.keys ++ sec.keys,
                  fir.filter.fold(
                    sec.filter)(
                    f => sec.filter.fold(f.some)(s => Free.roll(And[T, JoinFunc[T]](f, s)).some)))
              case -\/(Eq(l, r)) =>
                alignCondition(l, r).fold(
                  SimplifiedJoinCondition(Nil, fm.some))(
                  pair => SimplifiedJoinCondition(List(pair), None))
              case _ => SimplifiedJoinCondition(Nil, fm.some)
            }

          def mergeSides(jf: JoinFunc[T]): FreeMap[T] =
            jf >>= {
              case LeftSide  => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0)))
              case RightSide => Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1)))
            }

          val SimplifiedJoinCondition(keys, filter) = separateConditions(tj.on)
          GtoH(QC.inj(Map(filter.foldLeft(
            GtoH(EJ.inj(EquiJoin(
              tj.src,
              applyToBranch(tj.lBranch),
              applyToBranch(tj.rBranch),
              ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.left)))),
              ConcatArraysN(keys.map(k => Free.roll(MakeArray[T, FreeMap[T]](k.right)))),
              tj.f,
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide)))))))).embed)(
            (ej, filt) => GtoH(QC.inj(Filter(ej, mergeSides(filt)))).embed),
            mergeSides(tj.combine))))
        }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive: EqualT: ShowT, F[_]]
    (implicit QC: QScriptCore[T, ?] :<: F)
      : SimplifyJoin.Aux[T, QScriptCore[T, ?], F] =
    new SimplifyJoin[QScriptCore[T, ?]] {
      type IT[F[_]] = T [F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H)
          : QScriptCore[T, T[H]] => H[T[H]] = fa => GtoH(QC.inj(fa match {
            case Union(src, lb, rb) =>
              Union(src, applyToBranch(lb), applyToBranch(rb))
            case Drop(src, lb, rb) =>
              Drop(src, applyToBranch(lb), applyToBranch(rb))
            case Take(src, lb, rb) =>
              Take(src, applyToBranch(lb), applyToBranch(rb))
            case _ => fa
          }))
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive: EqualT: ShowT, F[_]]
    (implicit EJ: EquiJoin[T, ?] :<: F)
      : SimplifyJoin.Aux[T, EquiJoin[T, ?], F] =
    new SimplifyJoin[EquiJoin[T, ?]] {
      type IT[F[_]] = T [F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): EquiJoin[T, T[H]] => H[T[H]] =
        ej => GtoH(EJ.inj(EquiJoin(
          ej.src,
          applyToBranch(ej.lBranch),
          applyToBranch(ej.rBranch),
          ej.lKey,
          ej.rKey,
          ej.f,
          ej.combine)))
    }

  implicit def coproduct[T[_[_]], F[_], I[_], J[_]]
    (implicit I: SimplifyJoin.Aux[T, I, F], J: SimplifyJoin.Aux[T, J, F])
      : SimplifyJoin.Aux[T, Coproduct[I, J, ?], F] =
    new SimplifyJoin[Coproduct[I, J, ?]] {
      type IT[F[_]] = T[F]
      type G[A] = F[A]
      def simplifyJoin[H[_]: Functor](GtoH: G ~> H)
          : Coproduct[I, J, T[H]] => H[T[H]] =
        _.run.fold(I.simplifyJoin(GtoH), J.simplifyJoin(GtoH))
    }
}

abstract class SimplifyJoinInstances {
  implicit def inject[T[_[_]], F[_], I[_]](implicit F: F :<: I)
      : SimplifyJoin.Aux[T, F, I] =
    new SimplifyJoin[F] {
      type IT[F[_]] = T[F]
      type G[A] = I[A]

      def simplifyJoin[H[_]: Functor](GtoH: G ~> H): F[T[H]] => H[T[H]] =
        fa => GtoH(F.inj(fa))
    }
}
