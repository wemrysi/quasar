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

sealed trait SourcedPathable[T[_[_]], A] {
  def src: A
}

/** A data-level transformation.
  */
final case class Map[T[_[_]], A](src: A, f: FreeMap[T]) extends SourcedPathable[T, A]

/** Flattens nested structure, converting each value into a data set, which are
  * then unioned.
  *
  * `struct` is an expression that evaluates to an array or object, which is
  * then “exploded” into multiple values. `repair` is applied across the new
  * set, integrating the exploded values into the original set.
  */
final case class LeftShift[T[_[_]], A](
  src: A,
  struct: FreeMap[T],
  repair: JoinFunc[T])
    extends SourcedPathable[T, A]

/** Creates a new dataset, |a|+|b|, containing all of the entries from each of
  * the input sets, without any indication of which set they came from
  *
  * This could be handled as another join type, the anti-join
  * (T[EJson] \/ T[EJson] => T[EJson], specifically as `_.merge`), with the
  * condition being `κ(true)`,
  */
final case class Union[T[_[_]], A](
  src: A,
  lBranch: JoinBranch[T],
  rBranch: JoinBranch[T])
    extends SourcedPathable[T, A]

object SourcedPathable {
  implicit def equal[T[_[_]]](implicit eqTEj: Equal[T[EJson]]): Delay[Equal, SourcedPathable[T, ?]] =
    new Delay[Equal, SourcedPathable[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (Map(a1, f1), Map(a2, f2)) => f1 ≟ f2 && eq.equal(a1, a2)
          case (LeftShift(a1, s1, r1), LeftShift(a2, s2, r2)) =>
            eq.equal(a1, a2) && s1 ≟ s2 && r1 ≟ r2
          case (Union(a1, l1, r1), Union(a2, l2, r2)) =>
            eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2
          case (_, _) => false
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[SourcedPathable[T, ?]] =
    new Traverse[SourcedPathable[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: SourcedPathable[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[SourcedPathable[T, B]] =
        fa match {
          case Map(a, func)       => f(a) ∘ (Map[T, B](_, func))
          case LeftShift(a, s, r) => f(a) ∘ (LeftShift(_, s, r))
          case Union(a, l, r)     => f(a) ∘ (Union(_, l, r))
        }
    }

  implicit def show[T[_[_]]](implicit shEj: Show[T[EJson]]): Delay[Show, SourcedPathable[T, ?]] =
    new Delay[Show, SourcedPathable[T, ?]] {
      def apply[A](s: Show[A]): Show[SourcedPathable[T, A]] = Show.show(_ match {
        case Map(src, mf) => Cord("Map(") ++
          s.show(src) ++ Cord(",") ++
          mf.show ++ Cord(")")
        case LeftShift(src, struct, repair) => Cord("LeftShift(") ++
          s.show(src) ++ Cord(",") ++
          struct.show ++ Cord(",") ++
          repair.show ++ Cord(")")
        case Union(src, l, r) => Cord("Union(") ++
          s.show(src) ++ Cord(",") ++
          l.show ++ Cord(",") ++
          r.show ++ Cord(")")
      })
    }

  implicit def mergeable[T[_[_]]: Corecursive]:
      Mergeable.Aux[T, SourcedPathable[T, Unit]] =
    new Mergeable[SourcedPathable[T, Unit]] {
      type IT[F[_]] = T[F]

      val mf = new MapFuncs[IT, FreeMap[IT]]
      import mf._

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: SourcedPathable[IT, Unit],
        p2: SourcedPathable[IT, Unit]): Option[Merge[IT, SourcedPathable[IT, Unit]]] =
        (p1, p2) match {
          case (Map(_, m1), Map(_, m2)) => {
            val lf =
              Free.roll(ProjectField(UnitF[IT], Free.roll[MapFunc[IT, ?], Unit](StrLit("tmp1"))))
            val rf =
              Free.roll(ProjectField(UnitF[IT], Free.roll[MapFunc[IT, ?], Unit](StrLit("tmp2"))))

            AbsMerge[IT, SourcedPathable[IT, Unit], FreeMap](Map((), Free.roll[MapFunc[IT, ?], Unit](
              ConcatObjects(List(
                Free.roll[MapFunc[IT, ?], Unit](MakeObject(Free.roll(StrLit("tmp1")), rebase(m1, left))),
                Free.roll[MapFunc[IT, ?], Unit](MakeObject(Free.roll(StrLit("tmp2")), rebase(m2, right))))))),
              lf, rf).some
          }
          case _ => None
        }
    }
}
