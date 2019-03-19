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

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree, RenderedTree, Terminal}
import quasar.RenderTree.ops._

import cats.{Eq, Eval, Foldable, Show}
import cats.kernel.{BoundedSemilattice, CommutativeMonoid, CommutativeSemigroup, Semigroup}
import cats.instances.list._
import cats.syntax.eq._
import cats.syntax.foldable._
import cats.syntax.functor._
import cats.syntax.semigroup._
import cats.syntax.show._
import cats.syntax.traverse._

import monocle.PTraversal

import scalaz.{@@, Applicative}
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

/** A distinct union of products. */
final class Uop[A] private (val toList: List[A]) {
  import Uop.distinctE

  /** Alias for `and`. */
  def ∧ (that: Uop[A])(implicit asg: Semigroup[A], aeq: Eq[A]): Uop[A] =
    and(that)

  /** Alias for `or`. */
  def ∨ (that: Uop[A])(implicit A: Eq[A]): Uop[A] =
    or(that)

  /** The product of two `Uop`, distributing over the union. */
  def and(that: Uop[A])(implicit asg: Semigroup[A], aeq: Eq[A]): Uop[A] =
    if (isEmpty) {
      that
    } else if (that.isEmpty) {
      this
    } else {
      val as = for {
        thiss <- toList
        thats <- that.toList
      } yield thiss |+| thats

      if (hasMany(toList) || hasMany(that.toList))
        new Uop(distinctE(as))
      else
        new Uop(as)
    }

  def isEmpty: Boolean =
    toList.isEmpty

  def map[B: Eq](f: A => B): Uop[B] =
    new Uop(distinctE(toList map f))

  /** The union of two `Uop`. */
  def or(that: Uop[A])(implicit A: Eq[A]): Uop[A] =
    new Uop(distinctE(toList ::: that.toList))

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  override def toString: String = {
    implicit val renderA = RenderTree.make[A](a => Terminal(List(a.toString), None))
    this.show
  }

  ////

  private def hasMany[X](xs: List[X]): Boolean =
    xs match {
      case _ :: _ :: _ => true
      case _ => false
    }
}

object Uop extends UopInstances {
  def empty[A]: Uop[A] =
    new Uop(Nil)

  def fromFoldable[F[_]: Foldable, A: Eq](fa: F[A]): Uop[A] =
    new Uop(distinctE(fa))

  def of[A: Eq](as: A*): Uop[A] =
    fromFoldable(as.toList)

  def one[A](a: A): Uop[A] =
    new Uop(List(a))

  def values[A, B: Eq]: PTraversal[Uop[A], Uop[B], A, B] =
    new PTraversal[Uop[A], Uop[B], A, B] {
      import shims._

      def modifyF[F[_]: Applicative](f: A => F[B])(uop: Uop[A]): F[Uop[B]] =
        uop.toList.traverse(f).map(fromFoldable(_))
    }

  ////

  private def distinctE[F[_]: Foldable, A: Eq](fa: F[A]): List[A] =
    fa.foldr(Eval.now(List[A]())) { (a, ev) =>
      ev map { seen =>
        if (seen.exists(_ === a))
          seen
        else
          a :: seen
      }
    }.value
}

sealed abstract class UopInstances {
  implicit def conjCommutativeMonoid[A: CommutativeSemigroup: Eq]: CommutativeMonoid[Uop[A] @@ Conjunction] =
    new CommutativeMonoid[Uop[A] @@ Conjunction] {
      val empty = Conjunction(Uop.empty[A])

      def combine(x: Uop[A] @@ Conjunction, y: Uop[A] @@ Conjunction) =
        Conjunction(x.unwrap ∧ y.unwrap)
    }

  implicit def disjBoundedSemilattice[A: Eq]: BoundedSemilattice[Uop[A] @@ Disjunction] =
    new BoundedSemilattice[Uop[A] @@ Disjunction] {
      val empty = Disjunction(Uop.empty[A])

      def combine(x: Uop[A] @@ Disjunction, y: Uop[A] @@ Disjunction) =
        Disjunction(x.unwrap ∨ y.unwrap)
    }

  implicit def equal[A: Eq]: Eq[Uop[A]] =
    Eq.by(uop => AsSet(uop.toList))

  implicit def renderTree[A: RenderTree]: RenderTree[Uop[A]] =
    RenderTree make { uop =>
      NonTerminal(List("Union"), None, uop.toList.map(_.render))
    }

  implicit def show[A: RenderTree]: Show[Uop[A]] =
    Show.show(uop => scalaz.Show[RenderedTree].shows(uop.render))
}
