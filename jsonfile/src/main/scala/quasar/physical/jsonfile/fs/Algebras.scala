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

package quasar.physical.jsonfile.fs

import quasar.Predef._

trait StringAlgebra[A] {
  def toLower(s: A): A
  def toUpper(s: A): A
  def asBoolean(s: A): A
  def asInteger(s: A): A
  def asDecimal(s: A): A
  def search(s: A, pattern: A, insensitive: A): A
  def substring(s: A, offset: A, length: A): A
}
trait NumericAlgebra[A] {
  def asLong(x: A): Option[Long]
  def fromLong(x: Long): A
  def negate(x: A): A
  def plus(x: A, y: A): A
  def minus(x: A, y: A): A
  def times(x: A, y: A): A
  def div(x: A, y: A): A
  def mod(x: A, y: A): A
  def pow(x: A, y: A): A
}
trait BooleanAlgebra[A] {
  def one: A
  def zero: A
  def complement(a: A): A
  def and(a: A, b: A): A
  def or(a: A, b: A): A
}
trait TimeAlgebra[A] {
  def fromLong(x: Long): A
  def asZonedDateTime(x: A): ZonedDateTime
}

object NumericAlgebra {
  def apply[A](implicit z: NumericAlgebra[A]): NumericAlgebra[A] = z
}
object BooleanAlgebra {
  def apply[A](implicit z: BooleanAlgebra[A]): BooleanAlgebra[A] = z
}
