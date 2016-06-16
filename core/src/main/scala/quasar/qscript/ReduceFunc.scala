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

// TODO we should statically verify that these have `DimensionalEffect` of `Reduction`
sealed trait ReduceFunc[A]
final case class Count[A](a: A)     extends ReduceFunc[A]
final case class Sum[A](a: A)       extends ReduceFunc[A]
final case class First[A](a: A)     extends ReduceFunc[A]
final case class Last[A](a: A)      extends ReduceFunc[A]
final case class Min[A](a: A)       extends ReduceFunc[A]
final case class Max[A](a: A)       extends ReduceFunc[A]
final case class Avg[A](a: A)       extends ReduceFunc[A]
final case class Arbitrary[A](a: A) extends ReduceFunc[A]

object ReduceFunc {
  implicit val equal: Delay[Equal, ReduceFunc] =
    new Delay[Equal, ReduceFunc] {
      def apply[A](eq: Equal[A]) = Equal.equal {
        case (Count(a),     Count(b))     => eq.equal(a, b)
        case (First(a),     First(b))     => eq.equal(a, b)
        case (Last(a),      Last(b))      => eq.equal(a, b)
        case (Sum(a),       Sum(b))       => eq.equal(a, b)
        case (Min(a),       Min(b))       => eq.equal(a, b)
        case (Max(a),       Max(b))       => eq.equal(a, b)
        case (Avg(a),       Avg(b))       => eq.equal(a, b)
        case (Arbitrary(a), Arbitrary(b)) => eq.equal(a, b)
        case (_,            _)            => false
      }
    }
}
