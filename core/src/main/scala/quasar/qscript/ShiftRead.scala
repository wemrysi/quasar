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

import matryoshka._
import matryoshka.patterns.CoEnv
import scalaz._, Scalaz._

/** This optional transformation changes the semantics of [[Read]]. The default
  * semantics return a single value, whereas the transformed version has an
  * implied [[LeftShift]] and therefore returns a set of values, which more
  * closely matches the way many data stores behave.
  */
trait ShiftRead[F[_]] {
  type G[A]

  // NB: This could be either a futumorphism (as it is) or an apomorphism. Not
  //     sure if one is clearly better.
  def shiftRead[H[_]](GtoH: G ~> H): F ~> (H ∘ Free[H, ?])#λ
}

object ShiftRead extends ShiftReadInstances {
  // NB: The `T` here _looks_ unused, but if it’s not propagated in the type,
  //     then the `read` implicit can’t be found.
  type Aux[T[_[_]], F[_], H[_]] = ShiftRead[F] { type G[A] = H[A] }

  def apply[T[_[_]], F[_], G[_]](implicit ev: ShiftRead.Aux[T, F, G]) = ev

  def applyToBranch[T[_[_]]: Recursive: Corecursive](branch: FreeQS[T]):
      FreeQS[T] =
    freeTransFutu(branch)((co: CoEnv[Hole, QScriptTotal[T, ?], T[CoEnv[Hole, QScriptTotal[T, ?], ?]]]) => co.run.fold(
      κ(co.map(Free.point[CoEnv[Hole, QScriptTotal[T, ?], ?], T[CoEnv[Hole, QScriptTotal[T, ?], ?]]])),
      ShiftRead[T, QScriptTotal[T, ?], QScriptTotal[T, ?]].shiftRead(coenvPrism[QScriptTotal[T, ?], Hole].reverseGet)(_)))

  implicit def read[T[_[_]]: Recursive: Corecursive, F[_]]
    (implicit SR: Const[ShiftedRead, ?] :<: F, QC: QScriptCore[T, ?] :<: F)
      : ShiftRead.Aux[T, Const[Read, ?], F] =
    new ShiftRead[Const[Read, ?]] {
      type G[A] = F[A]
      def shiftRead[H[_]](GtoH: G ~> H): Const[Read, ?] ~> (H ∘ Free[H, ?])#λ =
        new (Const[Read, ?] ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](read: Const[Read, A]) =
            GtoH(QC.inj(Reduce(
              Free.liftF(GtoH(SR.inj(Const[ShiftedRead, A](ShiftedRead(
                read.getConst.path,
                IncludeId))))),
              NullLit(),
              List(ReduceFuncs.UnshiftMap(
                Free.roll(ProjectIndex(HoleF, IntLit(0))),
                Free.roll(ProjectIndex(HoleF, IntLit(1))))),
              Free.point(ReduceIndex(0)))))
        }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: Corecursive, F[_]](implicit QC: QScriptCore[T, ?] :<: F):
      ShiftRead.Aux[T, QScriptCore[T, ?], F] =
    new ShiftRead[QScriptCore[T, ?]] {
      type G[A] = F[A]
      def shiftRead[H[_]](GtoH: G ~> H): QScriptCore[T, ?] ~> (H ∘ Free[H, ?])#λ =
        new (QScriptCore[T, ?] ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](qc: QScriptCore[T, A]) = GtoH(QC.inj(qc match {
            case Union(src, lb, rb) =>
              Union(Free.point(src), applyToBranch(lb), applyToBranch(rb))
            case Drop(src, lb, rb) =>
              Drop(Free.point(src), applyToBranch(lb), applyToBranch(rb))
            case Take(src, lb, rb) =>
              Take(Free.point(src), applyToBranch(lb), applyToBranch(rb))
            case _ => qc.map(Free.point)
          }))
        }
    }

  implicit def thetaJoin[T[_[_]]: Recursive: Corecursive, F[_]](implicit TJ: ThetaJoin[T, ?] :<: F):
      ShiftRead.Aux[T, ThetaJoin[T, ?], F] =
    new ShiftRead[ThetaJoin[T, ?]] {
      type G[A] = F[A]
      def shiftRead[H[_]](GtoH: G ~> H): ThetaJoin[T, ?] ~> (H ∘ Free[H, ?])#λ =
        new (ThetaJoin[T, ?] ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](tj: ThetaJoin[T, A]) =
            GtoH(TJ.inj(ThetaJoin(
              Free.point(tj.src),
              applyToBranch(tj.lBranch),
              applyToBranch(tj.rBranch),
              tj.on,
              tj.f,
              tj.combine)))
        }
    }

  implicit def equiJoin[T[_[_]]: Recursive: Corecursive, F[_]](implicit EJ: EquiJoin[T, ?] :<: F):
      ShiftRead.Aux[T, EquiJoin[T, ?], F] =
    new ShiftRead[EquiJoin[T, ?]] {
      type G[A] = F[A]
      def shiftRead[H[_]](GtoH: G ~> H): EquiJoin[T, ?] ~> (H ∘ Free[H, ?])#λ =
        new (EquiJoin[T, ?] ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](ej: EquiJoin[T, A]) =
            GtoH(EJ.inj(EquiJoin(
              Free.point(ej.src),
              applyToBranch(ej.lBranch),
              applyToBranch(ej.rBranch),
              ej.lKey,
              ej.rKey,
              ej.f,
              ej.combine)))
        }
    }

  implicit def coproduct[T[_[_]], F[_], I[_], J[_]]
    (implicit I: ShiftRead.Aux[T, I, F], J: ShiftRead.Aux[T, J, F]):
      ShiftRead.Aux[T, Coproduct[I, J, ?], F] =
    new ShiftRead[Coproduct[I, J, ?]] {
      type G[A] = F[A]
      def shiftRead[H[_]](GtoH: G ~> H): Coproduct[I, J, ?] ~> (H ∘ Free[H, ?])#λ =
        new (Coproduct[I, J, ?] ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](co: Coproduct[I, J, A]) =
            co.run.fold(I.shiftRead(GtoH)(_), J.shiftRead(GtoH)(_))
        }
    }
}

abstract class ShiftReadInstances {
  implicit def inject[T[_[_]], F[_]: Functor, I[_]](implicit F: F :<: I):
      ShiftRead.Aux[T, F, I] =
    new ShiftRead[F] {
      type G[A] = I[A]
      def shiftRead[H[_]](GtoH: G ~> H): F ~> (H ∘ Free[H, ?])#λ =
        new (F ~> (H ∘ Free[H, ?])#λ) {
          def apply[A](fa: F[A]) = GtoH(F.inj(fa.map(Free.point)))
        }
    }
}
