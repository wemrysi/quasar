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

package quasar.physical.fallback.fs

trait BooleanAlgebra[A] {
  def one: A
  def zero: A
  def complement(a: A): A
  def and(a: A, b: A): A
  def or(a: A, b: A): A
}
object BooleanAlgebra {
  def apply[A](implicit z: BooleanAlgebra[A]): BooleanAlgebra[A] = z

  implicit object BooleanAlgebraData extends BooleanAlgebra[Data] {
    val one  = Data.Bool(true)
    val zero = Data.Bool(false)

    def complement(a: Data): Data = a match {
      case Data.Bool(x) => Data.Bool(!x)
      case _            => Data.NA
    }
    def and(a: Data, b: Data): Data = (a, b) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a && b)
      case _                            => Data.NA
    }
    def or(a: Data, b: Data): Data = (a, b) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a || b)
      case _                            => Data.NA
    }
  }
}
