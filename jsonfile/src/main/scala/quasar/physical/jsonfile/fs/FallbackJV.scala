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
import quasar.{ ejson => ej }
import matryoshka._
import scalaz._, Scalaz._
import ygg.json._
import InMemory.InMemState

object FallbackJV {
  implicit def liftIntJValue(x: Int): JValue       = JNum(x)
  implicit def liftBoolJValue(x: Boolean): JValue  = JBool(x)
  implicit def liftStringJValue(x: String): JValue = JString(x)

  implicit object JValueTimeAlgebra extends TimeAlgebra[JValue] {
    def fromLong(x: Long): JValue = JNum(x)
    def asZonedDateTime(x: JValue): ZonedDateTime = x match {
      case JNum(x) => zonedUtcFromMillis(x.longValue)
      case _       => throw new Exception(x.toString)
    }
  }

  implicit object JValueNumericAlgebra extends NumericAlgebra[JValue] {
    private type A = JValue
    private def binop(x: A, y: A)(f: (BigDecimal, BigDecimal) => BigDecimal): A = (x, y) match {
      case (JNum(a), JNum(b)) => JNum(f(a, b))
      case _                  => JUndefined
    }

    def negate(x: A): A      = x match { case JNum(a) => JNum(-a) ; case _ => JUndefined }
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

  implicit object JValueFallbackTypes extends FallbackTypes[JValue] {
    val undef  = JUndefined
    val bool   = partialPrism[JValue, Boolean] { case JBool(x) => x } ((x, s) => JBool(x))
    val string = partialPrism[JValue, String] { case JString(x) => x } ((x, s) => JString(x))
    val long   = partialPrism[JValue, Long] { case JNum(x) => x.longValue } ((x, s) => JNum(x))

    def hasType(value: JValue, tpe: Type): Boolean = (value, tpe) match {
      case (JBool(_), Type.Bool)  => true
      case (JString(_), Type.Str) => true
      case (JNum(x), Type.Int)    => x.isWhole
      case (JNum(x), Type.Dec)    => true
      case (JNull, Type.Null)     => true
      case _                      => false
    }
  }

  val fromCommon: Algebra[ej.Common, JValue] = {
    case ej.Arr(value)  => JArray(value)
    case ej.Null()      => JNull
    case ej.Bool(value) => JBool(value)
    case ej.Str(value)  => JString(value)
    case ej.Dec(value)  => JNum(value)
  }
  val fromExtension: Algebra[ej.Extension, JValue] = {
    case ej.Meta(value, meta) => value
    case ej.Map(value)        => JObject(value map { case (k, v) => (k.toString, v) } toMap)
    case ej.Byte(value)       => JNum(value.toInt)
    case ej.Char(value)       => JNum(value.toInt)
    case ej.Int(value)        => JNum(BigDecimal(value))
  }
  val fromEJson: Algebra[EJson, JValue] = _.run.fold(fromExtension, fromCommon)
  val toEJson: Coalgebra[EJson, JValue] = {
    import Coproduct._
    {
      case JNull       => right(ej.Null())
      case JString(s)  => right(ej.Str(s))
      case JBool(x)    => right(ej.Bool(x))
      case JNum(x)     => right(ej.Dec(x))
      case JArray(xs)  => right(ej.Arr(xs.toList))
      case JObject(xs) => left(ej.Map(xs.toList map (_ leftMap JString)))
    }
  }

  implicit def jvalueToEJson(x: JValue): EJson[JValue] = toEJson(x)

  def apply[T[_[_]]: Recursive : Corecursive]                                                    = Fallback.free[T, JValue](fromEJson, toEJson)
  def evalT[T[_[_]]: Recursive : Corecursive](mf: MapFunc[T, JValue], state: InMemState): JValue = apply[T].mapFunc(mf).eval(state)
  def eval(mf: MapFunc[Fix, JValue])                                                             = evalT[Fix](mf, InMemState.empty)
}

object jsonParser {
  import quasar.Predef._
  import quasar.{ejson => ejs}
  import jawn._

  def apply[T[_[_]]: Corecursive, F[_]: Functor](implicit C: ejs.Common :<: F, E: ejs.Extension :<: F): SupportParser[T[F]] =
    new SupportParser[T[F]] {
      implicit val facade: Facade[T[F]] =
        new SimpleFacade[T[F]] {
          def jarray(arr: List[T[F]])         = C(ejs.Arr(arr)).embed
          // TODO: Should `ListMap` really be in the interface, or just used as impl?
          def jobject(obj: Map[String, T[F]]) = E(ejs.Map(obj.toList.map(_.leftMap(k => C(ejs.Str[T[F]](k)).embed)))).embed
          def jnull()                         = C(ejs.Null[T[F]]()).embed
          def jfalse()                        = C(ejs.Bool[T[F]](false)).embed
          def jtrue()                         = C(ejs.Bool[T[F]](true)).embed
          def jnum(n: String)                 = C(ejs.Dec[T[F]](BigDecimal(n))).embed
          def jint(n: String)                 = E(ejs.Int[T[F]](BigInt(n))).embed
          def jstring(s: String)              = C(ejs.Str[T[F]](s)).embed
        }
    }
}
