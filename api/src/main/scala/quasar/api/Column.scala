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

package quasar.api

import slamdata.Predef._

import cats.{Applicative, Eq, Eval, Show, Traverse}
import cats.implicits._

final case class Column[T](name: String, tpe: T)

object Column {
  implicit val traverseColumn: Traverse[Column] =
    new Traverse[Column] {
      def foldLeft[A, B](fa: Column[A], b: B)(f: (B, A) => B): B =
        f(b, fa.tpe)

      def foldRight[A, B](fa: Column[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
        f(fa.tpe, lb)

      override def map[A, B](fa: Column[A])(f: A => B): Column[B] =
        fa.copy(tpe = f(fa.tpe))

      def traverse[G[_]: Applicative, A, B](fa: Column[A])(f: A => G[B]): G[Column[B]] =
        f(fa.tpe).map(b => fa.copy(tpe = b))
    }

  implicit def equalColumn[T: Eq]: Eq[Column[T]] =
    Eq.by(c => (c.name, c.tpe))

  implicit def showColumn[T: Show]: Show[Column[T]] =
    Show show { tc =>
      "Column(" + tc.name + ", " + tc.tpe.show + ")"
    }
}
