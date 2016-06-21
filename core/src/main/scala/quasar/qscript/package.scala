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

package quasar

import quasar.Predef._
import quasar.fp._

import scalaz._, Scalaz._

/** Here we no longer care about provenance. Backends can’t do anything with
  * it, so we simply represent joins and crosses directly. This also means that
  * we don’t need to model certain things – project_d is just a data-level
  * function, nest_d & swap_d only modify provenance and so are irrelevant
  * here, and autojoin_d has been replaced with a lower-level join operation
  * that doesn’t include the cross portion.
  */
package object qscript {
  type Pathable[T[_[_]], A] = Coproduct[Const[DeadEnd, ?], SourcedPathable[T, ?], A]

  /** These are the operations included in all forms of QScript.
    */
  type QScriptPrim[T[_[_]], A] = Coproduct[QScriptCore[T, ?], Pathable[T, ?], A]

  /** This is the target of the core compiler. Normalization is applied to this
    * structure, and it contains no Read or EquiJoin.
    */
  type QScriptPure[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptPrim[T, ?], A]

  /** These nodes exist in all QScript structures that a backend sees.
    */
  type QScriptCommon[T[_[_]], A] = Coproduct[Read, QScriptPrim[T, ?], A]

  // The following two types are the only ones that should be seen by a backend.

  /** This is the primary form seen by a backend. It contains reads of files.
    */
  type QScript[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptCommon[T, ?], A]

  /** A variant with a simpler join type. A backend can choose to operate on this
    * structure by applying the `equiJoinsOnly` transformation. Backends
    * without true join support will likely find it easier to work with this
    * than to handle full ThetaJoins.
    */
  type EquiQScript[T[_[_]], A] = Coproduct[EquiJoin[T, ?], QScriptCommon[T, ?], A]

  sealed trait JoinSide
  final case object LeftSide extends JoinSide
  final case object RightSide extends JoinSide

  object JoinSide {
    implicit val equal: Equal[JoinSide] = Equal.equalRef
    implicit val show: Show[JoinSide] = Show.showFromToString
  }

  type FreeMap[T[_[_]]] = Free[MapFunc[T, ?], Unit]
  type JoinFunc[T[_[_]]] = Free[MapFunc[T, ?], JoinSide]
  type JoinBranch[T[_[_]]] = Free[QScriptPure[T, ?], Unit]

  def UnitF[T[_[_]]] = Free.point[MapFunc[T, ?], Unit](())
  final case class AbsMerge[T[_[_]], A, Q[_[_[_]]]](
    src: A,
    left: Q[T],
    right: Q[T])

  type Merge[T[_[_]], A] = AbsMerge[T, A, FreeMap]
  type MergeJoin[T[_[_]], A] = AbsMerge[T, A, JoinBranch]

  // replace Unit in `in` with `field`
  def rebase[T[_[_]]](in: FreeMap[T], field: FreeMap[T]): FreeMap[T] = in >> field

  // TODO this should be found from matryoshka - why isn't it being found!?!?
  implicit def NTEqual[F[_], A](implicit F: Delay[Equal, F], A: Equal[A]):
      Equal[F[A]] =
    F(A)
  implicit def NTShow[F[_], A](implicit F: Delay[Show, F], A: Show[A]):
      Show[F[A]] =
    F(A)

  implicit def constMergeable[T[_[_]], A](
    implicit ma: Mergeable.Aux[T, A]): Mergeable.Aux[T, Const[A, Unit]] = new Mergeable[Const[A, Unit]] {
    type IT[F[_]] = T[F]

    def mergeSrcs(
      left: FreeMap[T],
      right: FreeMap[T],
      p1: Const[A, Unit],
      p2: Const[A, Unit]):
        Option[Merge[T, Const[A, Unit]]] =
      ma.mergeSrcs(left, right, p1.getConst, p2.getConst).map {
        case AbsMerge(src, l, r) => AbsMerge(Const(src), l, r)
      }
  }

  implicit def coproductMergeable[T[_[_]], F[_], G[_]](
    implicit mf: Mergeable.Aux[T, F[Unit]],
             mg: Mergeable.Aux[T, G[Unit]]):
      Mergeable.Aux[T, Coproduct[F, G, Unit]] =
    new Mergeable[Coproduct[F, G, Unit]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        cp1: Coproduct[F, G, Unit],
        cp2: Coproduct[F, G, Unit]): Option[Merge[IT, Coproduct[F, G, Unit]]] = {
        (cp1.run, cp2.run) match {
          case (-\/(left1), -\/(left2)) =>
            mf.mergeSrcs(left, right, left1, left2).map {
              case AbsMerge(src, left, right) => AbsMerge(Coproduct(-\/(src)), left, right)
            }
          case (\/-(right1), \/-(right2)) =>
            mg.mergeSrcs(left, right, right1, right2).map {
              case AbsMerge(src, left, right) => AbsMerge(Coproduct(\/-(src)), left, right)
            }
          case (_, _) => None
        }
      }
    }
}
