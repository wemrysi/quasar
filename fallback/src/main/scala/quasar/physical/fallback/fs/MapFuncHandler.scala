/*
 * Copyright 2014–2017 SlamData Inc.
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

import quasar.Predef._
import quasar.Planner.PlannerError
import quasar.{ ejson => ej }
import quasar.qscript.{ MapFunc, MapFuncs => mf }
import scalaz._, Scalaz._
import java.{ time => jt }
import java.time._
import matryoshka._, implicits._
import quasar.ejson.EJson

trait MapFuncHandler[A] extends Prisms[A] {
  implicit val ord: Order[A]
  implicit val show: Show[A]
  implicit val ba: BooleanAlgebra[A]
  implicit val na: NumericAlgebra[A]

  val jsonAlgebra: Algebra[EJson, A]

  val GetHour         = timeBased(_.getHour)
  val GetMicroseconds = timeBased(x => x.getSecond * 1000000 + x.getNano / 1000)
  val GetMilliseconds = timeBased(x => x.getSecond * 1000 + x.getNano / 1000000)
  val GetMinute       = timeBased(_.getMinute)
  val GetSecond       = timeBased(_.getSecond)
  val GetTimeOfDay    = timeBased(x => x)
  val GetEpochSeconds = instantBased(_.toEpochMilli / 1000)

  val GetDayOfMonth   = dateBased(_.getDayOfMonth)
  val GetDayOfWeek    = dateBased(_.getDayOfWeek.getValue)
  val GetDayOfYear    = dateBased(_.getDayOfYear)
  val GetMonth        = dateBased(_.getMonth.getValue)
  val GetYear         = dateBased(_.getYear)

  val GetCentury      = GetYear      map (_ /+ 100)  // [1,100]
  val GetDecade       = GetYear      map (_ /+ 10)   // [1,10]
  val GetIsoDayOfWeek = GetDayOfWeek map (_ + 1)     // [1,7]
  val GetIsoYear      = GetYear      map (x => x)    // ???
  val GetMillennium   = GetYear      map (_ /+ 1000) //
  val GetQuarter      = GetMonth     map (_ /+ 3)    // [1,4]
  val GetWeek         = GetDayOfYear map (_ /+ 7)    // [1,53]

  private def parseInstant(s: String): Instant     = jt.Instant parse s
  private def parseLocalDate(s: String): LocalDate = jt.LocalDate parse s
  private def parseLocalTime(s: String): LocalTime = jt.LocalTime parse s
  private def parseDuration(s: String): Duration   = jt.Duration parse s

  private def toInstant(v: LocalDate): Instant     = toInstant(v.atStartOfDay)
  private def toInstant(v: LocalDateTime): Instant = (v atZone UTC).toInstant
  private def toLocalDate(v: Instant): LocalDate   = (v atZone UTC).toLocalDate
  private def toLocalTime(v: Instant): LocalTime   = (v atZone UTC).toLocalTime

  private def instantBased(f: Instant => scala.Long) = Extractor.partial[A, scala.Long] {
    case Instant(v)   => f(v)
    case LocalDate(v) => f(toInstant(v))
  }
  private def dateBased[R](f: LocalDate => R) = Extractor.partial[A, R] {
    case LocalDate(v) => f(v)
    case Instant(v)   => f(toLocalDate(v))
  }
  private def timeBased[R](f: LocalTime => R) = Extractor.partial[A, R] {
    case LocalTime(v) => f(v)
    case Instant(v)   => f(toLocalTime(v))
  }

  def step[T[_[_]] : RecursiveT]: Algebra[MapFunc[T, ?], A] = {
    // TIME
    case mf.Date(Str(s))                            => LocalDate(parseLocalDate(s))
    case mf.Time(Str(s))                            => LocalTime(parseLocalTime(s))
    case mf.Timestamp(Str(s))                       => Instant(parseInstant(s))
    case mf.Interval(Str(s))                        => Duration(parseDuration(s))
    case mf.TimeOfDay(GetTimeOfDay(v))              => LocalTime(v)
    case mf.ToTimestamp(Int(ms))                    => Instant(jt.Instant ofEpochMilli ms.toLong)
    case mf.Now()                                   => Instant(now)
    case mf.ExtractCentury(GetCentury(n))           => Int(n)
    case mf.ExtractDayOfMonth(GetDayOfMonth(n))     => Int(n)
    case mf.ExtractDayOfWeek(GetDayOfWeek(n))       => Int(n)
    case mf.ExtractDayOfYear(GetDayOfYear(n))       => Int(n)
    case mf.ExtractDecade(GetDecade(n))             => Int(n)
    case mf.ExtractEpoch(GetEpochSeconds(n))        => Int(n)
    case mf.ExtractHour(GetHour(n))                 => Int(n)
    case mf.ExtractIsoDayOfWeek(GetIsoDayOfWeek(n)) => Int(n)
    case mf.ExtractIsoYear(GetIsoYear(n))           => Int(n)
    case mf.ExtractMicroseconds(GetMicroseconds(n)) => Int(n)
    case mf.ExtractMillennium(GetMillennium(n))     => Int(n)
    case mf.ExtractMilliseconds(GetMilliseconds(n)) => Int(n)
    case mf.ExtractMinute(GetMinute(n))             => Int(n)
    case mf.ExtractMonth(GetMonth(n))               => Int(n)
    case mf.ExtractQuarter(GetQuarter(n))           => Int(n)
    case mf.ExtractSecond(GetSecond(n))             => Int(n)
    case mf.ExtractWeek(GetWeek(n))                 => Int(n)
    case mf.ExtractYear(GetYear(n))                 => Int(n)
    // case mf.ExtractTimezone(time)                   => // in seconds east of UTC (???)
    // case mf.ExtractTimezoneHour(time)               => //
    // case mf.ExtractTimezoneMinute(time)             => //

    // ORDER
    case mf.Eq(l, r)  => Bool reverseGet l === r
    case mf.Neq(l, r) => Bool reverseGet l =/= r
    case mf.Lt(l, r)  => Bool reverseGet l < r
    case mf.Lte(l, r) => Bool reverseGet l <= r
    case mf.Gt(l, r)  => Bool reverseGet l > r
    case mf.Gte(l, r) => Bool reverseGet l >= r

    // LOGICAL
    case mf.Constant(ej.Bool(true))  => ba.one
    case mf.Constant(ej.Bool(false)) => ba.zero
    case mf.And(l, r)                => ba.and(l, r)
    case mf.Or(l, r)                 => ba.or(l, r)
    case mf.Not(v)                   => ba.complement(v)

    // STRINGS
    case mf.Bool(Str("true"))  => ba.one
    case mf.Bool(Str("false")) => ba.zero
    case mf.Null(Str("null"))  => Null
    case mf.Lower(Str(s))      => Str reverseGet s.toLowerCase
    case mf.Upper(Str(s))      => Str reverseGet s.toUpperCase
    case mf.Integer(Str(s))    => Int reverseGet BigInt(s)
    case mf.Decimal(Str(s))    => Dec reverseGet BigDecimal(s)
    case mf.Length(Str(s))     => Int reverseGet s.length
    case mf.ToString(x)        => Str reverseGet (show shows x)

    // NUMERIC
    case mf.Negate(n)      => na.negate(n)
    case mf.Add(l, r)      => na.plus(l, r)
    case mf.Multiply(l, r) => na.times(l, r)
    case mf.Subtract(l, r) => na.minus(l, r)
    case mf.Divide(l, r)   => na.div(l, r)
    case mf.Modulo(l, r)   => na.mod(l, r)
    case mf.Power(l, r)    => na.pow(l, r)

    // FIN
    case mf.Constant(f) => f cata jsonAlgebra
    case mf.Undefined() => undef
    case _              => undef
  }
}

class MapFuncData extends MapFuncHandler[Data] with Prisms[Data] {
  import Planner._

  type Err[A] = PlannerError \/ A

  val ba          = BooleanAlgebra.BooleanAlgebraData
  val na          = NumericAlgebra.NumericAlgebraData
  val ord         = implicitly[Ord[Data]]
  val show        = implicitly[Show[Data]]

  val jsonAlgebra = Data.fromEJson
  val Bool        = Data._bool
  val Dec         = Data._dec
  val Duration    = Data._interval
  val Instant     = Data._timestamp
  val Int         = Data._int
  val LocalDate   = Data._date
  val LocalTime   = Data._time
  val Str         = Data._str

  val Null  = Data.Null
  val undef = Data.NA
  def now() = jt.Instant.now()
}
