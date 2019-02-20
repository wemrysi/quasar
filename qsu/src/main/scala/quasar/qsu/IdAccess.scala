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

package quasar.qsu

import slamdata.Predef._
import quasar.RenderTree
import quasar.fp.symbolOrder

import monocle.{Prism, PPrism, Traversal}
import scalaz.{Applicative, Equal, Order, Show, Traverse}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.show._

/** Describes access to the various forms of ids. */
sealed abstract class IdAccess[A]

object IdAccess extends IdAccessInstances {
  final case class Bucket[A](of: Symbol, idx: Int) extends IdAccess[A]
  final case class GroupKey[A](of: Symbol, idx: Int) extends IdAccess[A]
  final case class Identity[A](of: Symbol) extends IdAccess[A]
  final case class Static[A](value: A) extends IdAccess[A]

  def bucket[A]: Prism[IdAccess[A], (Symbol, Int)] =
    Prism.partial[IdAccess[A], (Symbol, Int)] {
      case Bucket(s, i) => (s, i)
    } { case (s, i) => Bucket(s, i) }

  def groupKey[A]: Prism[IdAccess[A], (Symbol, Int)] =
    Prism.partial[IdAccess[A], (Symbol, Int)] {
      case GroupKey(s, i) => (s, i)
    } { case (s, i) => GroupKey(s, i) }

  def identity[A]: Prism[IdAccess[A], Symbol] =
    Prism.partial[IdAccess[A], Symbol] {
      case Identity(s) => s
    } (Identity(_))

  def static[A]: Prism[IdAccess[A], A] =
    staticP[A, A]

  def staticP[A, B]: PPrism[IdAccess[A], IdAccess[B], A, B] =
    PPrism[IdAccess[A], IdAccess[B], A, B] {
      case Bucket(s, i) => bucket[B](s, i).left
      case GroupKey(s, i) => groupKey[B](s, i).left
      case Identity(s) => identity[B](s).left
      case Static(a) => a.right
    } (Static(_))

  def symbols[A]: Traversal[IdAccess[A], Symbol] =
    new Traversal[IdAccess[A], Symbol] {
      def modifyF[F[_]: Applicative](f: Symbol => F[Symbol])(s: IdAccess[A]) =
        s match {
          case Bucket(s, i)   => f(s) map (bucket(_, i))
          case GroupKey(s, i) => f(s) map (groupKey(_, i))
          case Identity(s)    => f(s) map (identity(_))
          case Static(a)      => static(a).point[F]
        }
    }
}

sealed abstract class IdAccessInstances extends IdAccessInstances0 {
  import IdAccess._

  implicit def order[A: Order]: Order[IdAccess[A]] =
    Order.orderBy(generic[A](_))

  implicit def show[A: Show]: Show[IdAccess[A]] =
    Show.shows {
      case Bucket(s, i)   => s"Bucket($s[$i])"
      case GroupKey(s, i) => s"GroupKey($s[$i])"
      case Identity(s)    => s"Identity($s)"
      case Static(a)      => s"Static(${a.shows})"
    }

  implicit def renderTree[A: Show]: RenderTree[IdAccess[A]] =
    RenderTree.fromShowAsType("IdAccess")

  implicit val traverse: Traverse[IdAccess] =
    new Traverse[IdAccess] {
      def traverseImpl[F[_]: Applicative, A, B](fa: IdAccess[A])(f: A => F[B]) =
        IdAccess.staticP[A, B].modifyF(f)(fa)
    }
}

sealed abstract class IdAccessInstances0 {
  import IdAccess.{bucket, groupKey, identity, static}

  implicit def equal[A: Equal]: Equal[IdAccess[A]] =
    Equal.equalBy(generic[A](_))

  protected def generic[A](a: IdAccess[A]) =
    (bucket.getOption(a), groupKey.getOption(a), identity.getOption(a), static.getOption(a))
}
