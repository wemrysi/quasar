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
import quasar.fs._
import quasar.qscript.{ MapFunc, MapFuncs => mf }
import matryoshka._
import matryoshka.Recursive.ops._
import scalaz._, Scalaz._
import jawn.Facade
import ygg.json._
import InMemory.InMemState

object FallbackJV {
  implicit def liftIntJValue(x: Int): JNum       = JNum(x)
  implicit def liftBoolJValue(x: Boolean): JBool = JBool(x)

  implicit object JValueClassifer extends Classifier[JValue, Type] {
    def hasType(value: JValue, tpe: Type): Boolean = (value, tpe) match {
      case (JBool(_), Type.Bool)  => true
      case (JString(_), Type.Str) => true
      case (JNull, Type.Null)     => true
      case _                      => false
    }
  }

  implicit object JValueNumericAlgebra extends NumericAlgebra[JValue] {
    private type A = JValue
    private def binop(x: A, y: A)(f: (BigDecimal, BigDecimal) => BigDecimal): A = (x, y) match {
      case (JNum(a), JNum(b)) => JNum(f(a, b))
      case _                  => JUndefined
    }

    def negate(x: A): A = x match { case JNum(a) => JNum(-a) ; case _ => JUndefined }
    def plus(x: A, y: A): A  = binop(x, y)(_ + _)
    def minus(x: A, y: A): A = binop(x, y)(_ - _)
    def times(x: A, y: A): A = binop(x, y)(_ * _)
    def div(x: A, y: A): A   = binop(x, y)(_ / _)
    def mod(x: A, y: A): A   = binop(x, y)(_ % _)
    def pow(x: A, y: A): A   = binop(x, y)(_ pow _.intValue)
  }
  implicit object JValueBooleanAlgebra extends BooleanAlgebra[JValue] {
    private type A = JValue

    def one: A = JTrue
    def zero: A = JFalse

    def complement(a: A): A = a match {
      case JTrue  => JFalse
      case JFalse => JTrue
      case _      => JUndefined
    }
    def and(a: A, b: A): A = (a, b) match {
      case (JBool(a), JBool(b)) => JBool(a && b)
      case _                    => JUndefined
    }
    def or(a: A, b: A): A = (a, b) match {
      case (JBool(a), JBool(b)) => JBool(a || b)
      case _                    => JUndefined
    }
  }

  def apply[T[_[_]]: Recursive]                                                    = Fallback.free[T, JValue](JUndefined)
  def evalT[T[_[_]]: Recursive](mf: MapFunc[T, JValue], state: InMemState): JValue = apply[T].mapFunc(mf).eval(state)
  def eval(mf: MapFunc[Fix, JValue])                                               = evalT[Fix](mf, InMemState.empty)
}
