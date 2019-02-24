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

package quasar.qscript.provenance

import slamdata.Predef.Boolean
import quasar.fp.ski.ι

import monocle.{Iso, PIso, PTraversal, Traversal}
import monocle.function.Cons1
import scalaz.{@@, Applicative, Cord, Equal, ICons, IList, Monoid, NonEmptyList, SemiLattice, Show, Traverse}
import scalaz.Scalaz._
import scalaz.Tags.{Disjunction, Conjunction}
import scalaz.syntax.tag._

final case class Dimensions[A](union: IList[NonEmptyList[A]]) {
  def isEmpty: Boolean =
    union.isEmpty

  def nonEmpty: Boolean =
    !isEmpty

  def map[B](f: A => B): Dimensions[B] =
    Dimensions.pdimension.modify(f)(this)

  def mapJoin[B](f: NonEmptyList[A] => NonEmptyList[B]): Dimensions[B] =
    Dimensions.pjoin.modify(f)(this)

  def and(rhs: Dimensions[A])(implicit A0: Equal[A], A1: SemiLattice[A]): Dimensions[A] =
    if (isEmpty) rhs
    else if (rhs.isEmpty) this
    else {
      val conj = Dimensions(for {
        l <- union
        r <- rhs.union
      } yield l.reverse.alignWith(r.reverse)(_.fold(ι, ι, _ |+| _)).reverse)

      if (hasMany(union) && hasMany(rhs.union))
        Dimensions.normalize(conj)
      else
        conj
    }

  def ∧(rhs: Dimensions[A])(implicit A0: Equal[A], A1: SemiLattice[A]): Dimensions[A] =
    and(rhs)

  def or(rhs: Dimensions[A])(implicit A: Equal[A]): Dimensions[A] =
    Dimensions.normalize(Dimensions(union ++ rhs.union))

  def ∨(rhs: Dimensions[A])(implicit A: Equal[A]): Dimensions[A] =
    or(rhs)

  ////

  private def hasMany(xs: IList[_]): Boolean =
    xs match {
      case ICons(_, ICons(_, _)) => true
      case _ => false
    }
}

object Dimensions extends DimensionsInstances {
  def empty[A]: Dimensions[A] =
    Dimensions(IList.empty[NonEmptyList[A]])

  def origin[A](a: A, as: A*): Dimensions[A] =
    origin1(NonEmptyList.nels(a, as: _*))

  def origin1[A](as: NonEmptyList[A]): Dimensions[A] =
    Dimensions(IList(as))

  def pdimension[A, B]: PTraversal[Dimensions[A], Dimensions[B], A, B] =
    PTraversal.fromTraverse[Dimensions, A, B]

  def dimension[A]: Traversal[Dimensions[A], A] =
    pdimension[A, A]

  def pjoin[A, B]: PTraversal[Dimensions[A], Dimensions[B], NonEmptyList[A], NonEmptyList[B]] =
    punion.composeTraversal(PTraversal.fromTraverse[IList, NonEmptyList[A], NonEmptyList[B]])

  def join[A]: Traversal[Dimensions[A], NonEmptyList[A]] =
    pjoin[A, A]

  def topDimension[A]: Traversal[Dimensions[A], A] =
    join[A] composeLens Cons1.head

  def punion[A, B]: PIso[Dimensions[A], Dimensions[B], IList[NonEmptyList[A]], IList[NonEmptyList[B]]] =
    PIso((_: Dimensions[A]).union)(apply(_))

  def union[A]: Iso[Dimensions[A], IList[NonEmptyList[A]]] =
    punion[A, A]

  def normalize[A: Equal](ds: Dimensions[A]): Dimensions[A] =
    Dimensions(ds.union.distinctE)
}

sealed abstract class DimensionsInstances {
  // TODO: Actually a BoundedSemilattice, rewrite using type from cats.
  implicit def disjMonoid[A: Equal]: Monoid[Dimensions[A] @@ Disjunction] =
    Monoid.instance(
      (l, r) => Disjunction(l.unwrap ∨ r.unwrap),
      Disjunction(Dimensions.empty[A]))

  // TODO: Actually a CommutativeMonoid, rewrite using type from cats.
  implicit def conjMonoid[A: Equal: SemiLattice]: Monoid[Dimensions[A] @@ Conjunction] =
    Monoid.instance(
      (l, r) => Conjunction(l.unwrap ∧ r.unwrap),
      Conjunction(Dimensions.empty[A]))

  implicit def equal[A: Equal]: Equal[Dimensions[A]] =
    Equal.equalBy(d => AsSet(d.union))

  implicit def show[A: Show]: Show[Dimensions[A]] =
    Show.show(d => Cord("(") ++ d.union.map(_.show).intercalate(Cord(" ∨ ")) ++ Cord(")"))

  implicit val traverse: Traverse[Dimensions] =
    new Traverse[Dimensions] {
      val T = Traverse[IList].compose[NonEmptyList]
      def traverseImpl[F[_]: Applicative, A, B](fa: Dimensions[A])(f: A => F[B]) =
        T.traverse(fa.union)(f).map(Dimensions(_))
    }
}
