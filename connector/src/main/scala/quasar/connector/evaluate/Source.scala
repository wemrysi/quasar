/*
 * Copyright 2020 Precog Data
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

package quasar.connector.evaluate

import slamdata.Predef.StringContext

import quasar.api.resource.ResourcePath

import monocle.macros.Lenses

import cats.{Apply, Eq, Eval, NonEmptyTraverse, Order, Show}
import cats.implicits._

import shims.{equalToCats, orderToCats, showToCats}

@Lenses
final case class Source[A](path: ResourcePath, src: A) {
  def map[B](f: A => B): Source[B] =
    Source(path, f(src))
}

object Source extends SourceInstances

sealed abstract class SourceInstances extends SourceInstances0 {
  implicit def sourceOrder[A: Order]: Order[Source[A]] =
    Order.by {
      case Source(p, a) => (p, a)
    }

  implicit def sourceShow[A: Show]: Show[Source[A]] =
    Show show {
      case Source(p, a) => s"Source(${p.show}, ${a.show})"
    }

  implicit val sourceNonEmptyTraverse: NonEmptyTraverse[Source] =
    new NonEmptyTraverse[Source] {
      override def map[A, B](sa: Source[A])(f: A => B) =
        sa map f

      def nonEmptyTraverse[F[_]: Apply, A, B](fa: Source[A])(f: A => F[B]) =
        f(fa.src) map (Source(fa.path, _))

      def foldLeft[A, B](fa: Source[A], b: B)(f: (B, A) => B): B =
        f(b, fa.src)

      def foldRight[A, B](fa: Source[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        f(fa.src, lb)

      def reduceLeftTo[A, B](fa: Source[A])(f: A => B)(g: (B, A) => B): B =
        f(fa.src)

      def reduceRightTo[A, B](fa: Source[A])(f: A => B)(g: (A, Eval[B]) => Eval[B]): Eval[B] =
        Eval.now(f(fa.src))
    }
}

sealed abstract class SourceInstances0 {
  implicit def sourceEq[A: Eq]: Eq[Source[A]] =
    Eq by {
      case Source(p, a) => (p, a)
    }
}
