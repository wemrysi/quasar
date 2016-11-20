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
 import quasar.qscript.{ MapFuncs => mf }
 import scalaz.Scalaz._
 import java.time._

trait EvalMapFunc[T[_[_]], A] extends quasar.qscript.TTypes[T] {
  implicit def corecursive: Corecursive[T]
  implicit def recursive: Recursive[T]
  implicit def booleanAlgebra: BooleanAlgebra[A]
  implicit def numericAlgebra: NumericAlgebra[A]
  implicit def timeAlgebra: TimeAlgebra[A]
  implicit def order: Ord[A]
  // implicit def monad: Monad[F]
  // implicit def facade: jawn.Facade[A]

  implicit val StrPrism    : Prism[A, String]
  implicit val RegexPrism  : Prism[A, Regex]
  implicit val BoolPrism   : Prism[A, Boolean]
  implicit val DecPrism    : Prism[A, BigDecimal]
  implicit val Int32Prism  : Prism[A, Int]
  implicit val Int64Prism  : Prism[A, Long]
  implicit val BigIntPrism : Prism[A, BigInt]
  implicit val SymPrism    : Prism[A, scala.Symbol]

  implicit def liftPrism[A, B](x: B)(implicit z: Prism[A, B]): A = z reverseGet x

  def undef: A
  def nullA: A

  val stepMF: Algebra[MapFunc, A] = {
    // Boolean operations exploit the order and the boolean algebra.
    case mf.And(x, y)          => x && y
    case mf.Between(x, lo, hi) => lo <= x && x <= hi
    case mf.Eq(x, y)           => x === y
    case mf.Gt(x, y)           => x > y
    case mf.Gte(x, y)          => x >= y
    case mf.Lt(x, y)           => x < y
    case mf.Lte(x, y)          => x <= y
    case mf.Neq(x, y)          => x =/= y
    case mf.Not(x)             => !x
    case mf.Or(x, y)           => x || y
    case mf.Within(item, arr)  => TODO

    // Numeric operations exploit the numeric algebra.
    case mf.Add(x, y)      => x + y
    case mf.Divide(x, y)   => x / y
    case mf.Modulo(x, y)   => x % y
    case mf.Multiply(x, y) => x * y
    case mf.Negate(x)      => -x
    case mf.Power(b, e)    => b ** e
    case mf.Subtract(x, y) => x - y

    // String operations
    case mf.Bool(StrPrism("true"))                                   => true
    case mf.Bool(StrPrism("false"))                                  => false
    case mf.Decimal(StrPrism(s))                                     => BigDecimal(s)
    case mf.Integer(StrPrism(s))                                     => BigInt(s)
    case mf.Length(StrPrism(s))                                      => s.length
    case mf.ToString(StrPrism(s))                                    => s
    case mf.Lower(StrPrism(s))                                       => s.toLowerCase
    case mf.Null(StrPrism(s))                                        => (null: String)
    case mf.Search(x, RegexPrism(pattern), BoolPrism(insensitive))   => TODO
    case mf.Substring(StrPrism(s), Int32Prism(off), Int32Prism(len)) => (s drop off take len): String
    case mf.Upper(StrPrism(s))                                       => s.toUpperCase

    // Time operations
    case mf.Date(s)                         => TODO
    case mf.ExtractCentury(time)            => time.extractCentury
    case mf.ExtractDayOfMonth(time)         => time.extractDayOfMonth
    case mf.ExtractDayOfWeek(time)          => time.extractDayOfWeek
    case mf.ExtractDayOfYear(time)          => time.extractDayOfYear
    case mf.ExtractDecade(time)             => time.extractDecade
    case mf.ExtractEpoch(time)              => time.extractEpoch
    case mf.ExtractHour(time)               => time.extractHour
    case mf.ExtractIsoDayOfWeek(time)       => time.extractIsoDayOfWeek
    case mf.ExtractIsoYear(time)            => time.extractIsoYear
    case mf.ExtractMicroseconds(time)       => time.extractMicroseconds
    case mf.ExtractMillennium(time)         => time.extractMillennium
    case mf.ExtractMilliseconds(time)       => time.extractMilliseconds
    case mf.ExtractMinute(time)             => time.extractMinute
    case mf.ExtractMonth(time)              => time.extractMonth
    case mf.ExtractQuarter(time)            => time.extractQuarter
    case mf.ExtractSecond(time)             => time.extractSecond
    case mf.ExtractTimezone(time)           => time.extractTimezone
    case mf.ExtractTimezoneHour(time)       => time.extractTimezoneHour
    case mf.ExtractTimezoneMinute(time)     => time.extractTimezoneMinute
    case mf.ExtractWeek(time)               => time.extractWeek
    case mf.ExtractYear(time)               => time.extractYear
    case mf.Interval(s)                     => TODO
    case mf.Now()                           => nowMillis
    case mf.Time(s)                         => TODO
    case mf.TimeOfDay(dt)                   => TODO
    case mf.Timestamp(StrPrism(s))           => instantMillis(Instant.parse(s))
    case mf.ToTimestamp(Int64Prism(millis)) => zonedUtcFromMillis(millis).toString

    case mf.ConcatArrays(x, y)            => ??? // ejson.arrayConcat[F] apply (x, y)
    case mf.ConcatMaps(x, y)              => ??? // ejson.objectConcat[F] apply (x, y)
    case mf.Cond(p, t, f)                 => ??? // if_(p).then_(t).else_(f).point[F]
    case mf.Constant(ejson)               => ???
    case mf.DeleteField(src, field)       => ???
    case mf.Guard(value, tpe, ifp, elsep) => ??? // s"(: GUARD CONT :)$cont".xqy.point[F]
    case mf.IfUndefined(x, alternate)     => ??? // if_(fn.empty(x)).then_(alternate).else_(x).point[F]
    case mf.MakeArray(x)                  => ??? // ejson.singletonArray[F] apply x
    case mf.MakeMap(k, v)                 => abort(s"mf.MakeMap($k, $v)") // fromJValues(json"{ $k: $v }")
    case mf.ProjectField(src, field)      => abort(s"mf.ProjectField($src, $field)")
    case mf.ProjectIndex(arr, idx)        => ??? // ejson.arrayElementAt[F] apply (arr, idx + 1.xqy)
    case mf.Range(x, y)                   => ??? // (x to y).point[F]
    case mf.Undefined()                   => ??? // emptySeq.point[F]
  }
}
