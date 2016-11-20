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

import ygg._, common._

trait NumericAlgebra[A] {
  def negate(x: A): A
  def plus(x: A, y: A): A
  def minus(x: A, y: A): A
  def times(x: A, y: A): A
  def div(x: A, y: A): A
  def mod(x: A, y: A): A
  def pow(x: A, y: A): A
}
trait BooleanAlgebra[A] {
  def toBool(x: A): Option[Boolean]
  def fromBool(x: Boolean): A
  def complement(a: A): A
  def and(a: A, b: A): A
  def or(a: A, b: A): A
}
trait TimeAlgebra[A] {
  def fromLong(x: Long): A
  def asZonedDateTime(x: A): ZonedDateTime
}
trait StringAlgebra[A] {
  def intoString(x: A): Option[String]
  def fromString(s: String): A
  def onStringRep(a: A)(f: String => String): A
  def fromStringRep[B](a: A)(f: String => B): Option[B]
}

object NumericAlgebra {
  def apply[A](implicit z: NumericAlgebra[A]): NumericAlgebra[A] = z

  def undefined[A](expr: => A): NumericAlgebra[A] = new NumericAlgebra[A] {
    def negate(x: A): A      = expr
    def plus(x: A, y: A): A  = expr
    def minus(x: A, y: A): A = expr
    def times(x: A, y: A): A = expr
    def div(x: A, y: A): A   = expr
    def mod(x: A, y: A): A   = expr
    def pow(x: A, y: A): A   = expr
  }
}
object BooleanAlgebra {
  def apply[A](implicit z: BooleanAlgebra[A]): BooleanAlgebra[A] = z

  def undefined[A](expr: => A): BooleanAlgebra[A] = new BooleanAlgebra[A] {
    def toBool(x: A)            = None
    def fromBool(x: Boolean): A = expr
    def complement(a: A): A     = expr
    def and(a: A, b: A): A      = expr
    def or(a: A, b: A): A       = expr
  }
}
object TimeAlgebra {
  def apply[A](implicit z: TimeAlgebra[A]): TimeAlgebra[A] = z

  def undefined[A](expr: => A, zdt: => ZonedDateTime): TimeAlgebra[A] = new TimeAlgebra[A] {
    def fromLong(x: Long): A                 = expr
    def asZonedDateTime(x: A): ZonedDateTime = zdt
  }
}
object StringAlgebra {
  def apply[A](implicit z: StringAlgebra[A]): StringAlgebra[A] = z

  def undefined[A](expr: => A): StringAlgebra[A] = new StringAlgebra[A] {
    def intoString(x: A): Option[String]                  = None
    def fromString(s: String): A                          = expr
    def onStringRep(a: A)(f: String => String): A         = expr
    def fromStringRep[B](a: A)(f: String => B): Option[B] = None
  }
}
