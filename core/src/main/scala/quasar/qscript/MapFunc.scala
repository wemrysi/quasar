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

import quasar.fp._

import scalaz._

sealed trait MapFunc[T[_[_]], A]
final case class Nullary[T[_[_]], A](value: T[EJson]) extends MapFunc[T, A]
final case class Unary[T[_[_]], A](a1: A) extends MapFunc[T, A]
final case class Binary[T[_[_]], A](a1: A, a2: A) extends MapFunc[T, A]
final case class Ternary[T[_[_]], A](a1: A, a2: A, a3: A) extends MapFunc[T, A]

object MapFunc {
  implicit def equal[T[_[_]], A](implicit eqTEj: Equal[T[EJson]]): Delay[Equal, MapFunc[T, ?]] = new Delay[Equal, MapFunc[T, ?]] {
    // TODO this is wrong - we need to define equality on a function by function basis
    def apply[A](in: Equal[A]): Equal[MapFunc[T, A]] = Equal.equal {
      case (Nullary(v1), Nullary(v2)) => v1.equals(v2)
      case (Unary(a1), Unary(a2)) => in.equal(a1, a2)
      case (Binary(a11, a12), Binary(a21, a22)) => in.equal(a11, a21) && in.equal(a12, a22)
      case (Ternary(a11, a12, a13), Ternary(a21, a22, a23)) => in.equal(a11, a21) && in.equal(a12, a22) && in.equal(a13, a23)
      case (_, _) => false
    }
  }

  implicit def functor[T[_[_]]]: Functor[MapFunc[T, ?]] = new Functor[MapFunc[T, ?]] {
    def map[A, B](fa: MapFunc[T, A])(f: A => B): MapFunc[T, B] =
      fa match {
        case Nullary(v) => Nullary[T, B](v)
        case Unary(a1) => Unary(f(a1))
        case Binary(a1, a2) => Binary(f(a1), f(a2))
        case Ternary(a1, a2, a3) => Ternary(f(a1), f(a2), f(a3))
      }
  }

  implicit def show[T[_[_]]](implicit shEj: Show[T[EJson]]): Delay[Show, MapFunc[T, ?]] =
    new Delay[Show, MapFunc[T, ?]] {
      def apply[A](sh: Show[A]): Show[MapFunc[T, A]] = Show.show {
        case Nullary(v) => Cord("Nullary(") ++ shEj.show(v) ++ Cord(")")
        case Unary(a1) => Cord("Unary(") ++ sh.show(a1) ++ Cord(")")
        case Binary(a1, a2) => Cord("Binary(") ++ sh.show(a1) ++ sh.show(a2) ++ Cord(")")
        case Ternary(a1, a2, a3) => Cord("Ternary(") ++ sh.show(a1) ++ sh.show(a2) ++ sh.show(a3) ++ Cord(")")
      }
    }
}
