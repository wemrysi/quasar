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
import quasar.Type
import quasar.fs._
import quasar.qscript.{ MapFuncs => mf }
import quasar.ejson.EJson
import matryoshka._
import scalaz._, Scalaz._

trait StringAlgebra[A] {
  def toLower(s: A): A
  def toUpper(s: A): A
  def asBoolean(s: A): A
  def asInteger(s: A): A
  def asDecimal(s: A): A
  def search(s: A, pattern: A, insensitive: A): A
  def substring(s: A, offset: A, length: A): A
}
trait TimeAlgebra[A] {
  import java.time._
  import java.time.temporal.IsoFields

  protected implicit def fromInteger(x: Long): A
  protected implicit def toZonedDateTime(x: A): ZonedDateTime

  private def toZoneOffset(x: A): ZoneOffset = toZonedDateTime(x).getZone match { case x: ZoneOffset => x }

  // val ExtractCentury        = extract("Pulls out the century subfield from a date/time value (currently year/100).")
  // val ExtractDayOfMonth     = extract("Pulls out the day of month (`day`) subfield from a date/time value (1-31).")
  // val ExtractDayOfWeek      = extract("Pulls out the day of week (`dow`) subfield from a date/time value " +"(Sunday: 0 to Saturday: 7).")
  // val ExtractDayOfYear      = extract("Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).")
  // val ExtractDecade         = extract("Pulls out the decade subfield from a date/time value (year/10).")
  // val ExtractEpoch          = extract("Pulls out the epoch subfield from a date/time value. For dates and timestamps, this is the number of seconds since midnight, 1970-01-01. For intervals, the number of seconds in the interval.")
  // val ExtractHour           = extract("Pulls out the hour subfield from a date/time value (0-23).")
  // val ExtractIsoDayOfWeek   = extract("Pulls out the ISO day of week (`isodow`) subfield from a date/time value " +"(Monday: 1 to Sunday: 7).")
  // val ExtractIsoYear        = extract("Pulls out the ISO year (`isoyear`) subfield from a date/time value (based " +"on the first week containing Jan. 4).")
  // val ExtractMicroseconds   = extract("Pulls out the microseconds subfield from a date/time value (including seconds).")
  // val ExtractMillennium     = extract("Pulls out the millennium subfield from a date/time value (currently year/1000).")
  // val ExtractMilliseconds   = extract("Pulls out the milliseconds subfield from a date/time value (including seconds).")
  // val ExtractMinute         = extract("Pulls out the minute subfield from a date/time value (0-59).")
  // val ExtractMonth          = extract("Pulls out the month subfield from a date/time value (1-12).")
  // val ExtractQuarter        = extract("Pulls out the quarter subfield from a date/time value (1-4).")
  // val ExtractSecond         = extract("Pulls out the second subfield from a date/time value (0-59, with fractional parts).")
  // val ExtractTimezone       = extract("Pulls out the timezone subfield from a date/time value (in seconds east of UTC).")
  // val ExtractTimezoneHour   = extract("Pulls out the hour component of the timezone subfield from a date/time value.")
  // val ExtractTimezoneMinute = extract("Pulls out the minute component of the timezone subfield from a date/time value.")
  // val ExtractWeek           = extract("Pulls out the week subfield from a date/time value (1-53).")
  // val ExtractYear           = extract("Pulls out the year subfield from a date/time value.")

  def extractCentury(x: A): A        = x.getYear / 100
  def extractDayOfMonth(x: A): A     = x.getDayOfMonth
  def extractDayOfWeek(x: A): A      = x.getDayOfWeek.getValue % 7
  def extractDayOfYear(x: A): A      = x.getDayOfYear
  def extractDecade(x: A): A         = x.getYear / 10
  def extractEpoch(x: A): A          = toZonedDateTime(x).toInstant.getEpochSecond
  def extractHour(x: A): A           = x.getHour
  def extractIsoDayOfWeek(x: A): A   = x.getDayOfWeek.getValue
  def extractIsoYear(x: A): A        = x.getYear
  def extractMicroseconds(x: A): A   = x.getSecond * 1000000
  def extractMillennium(x: A): A     = x.getYear / 1000
  def extractMilliseconds(x: A): A   = x.getSecond * 1000
  def extractMinute(x: A): A         = x.getMinute
  def extractMonth(x: A): A          = x.getMonth.getValue
  def extractQuarter(x: A): A        = toZonedDateTime(x) get IsoFields.QUARTER_OF_YEAR
  def extractSecond(x: A): A         = x.getSecond
  def extractTimezone(x: A): A       = toZoneOffset(x).getTotalSeconds
  def extractTimezoneHour(x: A): A   = toZoneOffset(x).getTotalSeconds / (60 * 60)
  def extractTimezoneMinute(x: A): A = toZoneOffset(x).getTotalSeconds / 60
  def extractWeek(x: A): A           = toZonedDateTime(x) get IsoFields.WEEK_OF_WEEK_BASED_YEAR
  def extractYear(x: A): A           = x.getYear
}

trait NumericAlgebra[A] {
  def negate(x: A): A
  def plus(x: A, y: A): A
  def minus(x: A, y: A): A
  def times(x: A, y: A): A
  def div(x: A, y: A): A
  def mod(x: A, y: A): A
  def pow(x: A, y: A): A
}
object NumericAlgebra {
  def apply[A](implicit z: NumericAlgebra[A]) = z

  def lift[F[_]: Monad, A](alg: NumericAlgebra[A]): NumericAlgebra[F[A]] = new NumericAlgebra[F[A]] {
    def negate(x: F[A]): F[A]         = x map alg.negate
    def plus(x: F[A], y: F[A]): F[A]  = (x |@| y)(alg.plus)
    def minus(x: F[A], y: F[A]): F[A] = (x |@| y)(alg.minus)
    def times(x: F[A], y: F[A]): F[A] = (x |@| y)(alg.times)
    def div(x: F[A], y: F[A]): F[A]   = (x |@| y)(alg.div)
    def mod(x: F[A], y: F[A]): F[A]   = (x |@| y)(alg.mod)
    def pow(x: F[A], y: F[A]): F[A]   = (x |@| y)(alg.pow)
  }
  implicit class NumericAlgebraOps[A](private val self: A) {
    def unary_-(implicit alg: NumericAlgebra[A]): A     = alg.negate(self)
    def +(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.plus(self, that)
    def -(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.minus(self, that)
    def *(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.times(self, that)
    def /(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.div(self, that)
    def %(that: A)(implicit alg: NumericAlgebra[A]): A  = alg.mod(self, that)
    def **(that: A)(implicit alg: NumericAlgebra[A]): A = alg.pow(self, that)
  }
}

trait OrderAlgebra[A] {
  def eqv(x: A, y: A): A
  def neqv(x: A, y: A): A
  def lt(x: A, y: A): A
  def lte(x: A, y: A): A
  def gt(x: A, y: A): A
  def gte(x: A, y: A): A
}
object OrderAlgebra {
  def apply[A](implicit z: OrderAlgebra[A]) = z

  def lift[F[_]: Monad, A](alg: OrderAlgebra[A]): OrderAlgebra[F[A]] = new OrderAlgebra[F[A]] {
    def eqv(x: F[A], y: F[A]): F[A]  = (x |@| y)(alg.eqv)
    def neqv(x: F[A], y: F[A]): F[A] = (x |@| y)(alg.neqv)
    def lt(x: F[A], y: F[A]): F[A]   = (x |@| y)(alg.lt)
    def lte(x: F[A], y: F[A]): F[A]  = (x |@| y)(alg.lte)
    def gt(x: F[A], y: F[A]): F[A]   = (x |@| y)(alg.gt)
    def gte(x: F[A], y: F[A]): F[A]  = (x |@| y)(alg.gte)
  }
  implicit class OrderAlgebraOps[A](private val self: A) {
    def <(that: A)(implicit alg: OrderAlgebra[A]): A   = alg.lt(self, that)
    def <=(that: A)(implicit alg: OrderAlgebra[A]): A  = alg.lte(self, that)
    def >(that: A)(implicit alg: OrderAlgebra[A]): A   = alg.gt(self, that)
    def >=(that: A)(implicit alg: OrderAlgebra[A]): A  = alg.gte(self, that)
    def ===(that: A)(implicit alg: OrderAlgebra[A]): A = alg.eqv(self, that)
    def =/=(that: A)(implicit alg: OrderAlgebra[A]): A = alg.neqv(self, that)
  }
}

trait BooleanAlgebra[A] {
  def one: A
  def zero: A
  def complement(a: A): A
  def and(a: A, b: A): A
  def or(a: A, b: A): A
}
object BooleanAlgebra {
  def apply[A](implicit z: BooleanAlgebra[A]) = z

  def lift[F[_]: Monad, A](alg: BooleanAlgebra[A]): BooleanAlgebra[F[A]] = new BooleanAlgebra[F[A]] {
    def one: F[A]                   = alg.one.point[F]
    def zero: F[A]                  = alg.zero.point[F]
    def complement(a: F[A]): F[A]   = a map alg.complement
    def and(a: F[A], b: F[A]): F[A] = (a |@| b)(alg.and)
    def or(a: F[A], b: F[A]): F[A]  = (a |@| b)(alg.or)
  }
  implicit class BooleanAlgebraOps[A](private val self: A) {
    def unary_!(implicit alg: BooleanAlgebra[A]): A     = alg.complement(self)
    def &&(that: A)(implicit alg: BooleanAlgebra[A]): A = alg.and(self, that)
    def ||(that: A)(implicit alg: BooleanAlgebra[A]): A = alg.or(self, that)
  }
}

trait Extractor[A, B] {
  def unapply(x: A): Option[B]
}
object Extractor {
  def partial[A, B](pf: PartialFunction[A, B]): Extractor[A, B] = apply(pf.lift)
  def apply[A, B](f: A => Option[B]): Extractor[A, B]           = new Extractor[A, B] { def unapply(x: A) = f(x) }
}

abstract class PExtractor[A, B](pf: PartialFunction[A, B]) extends Extractor[A, B] {
  def unapply(x: A): Option[B] = pf lift x
}

trait Fresh[T[_[_]], F[_], Rep] extends quasar.qscript.TTypes[T] {
  def TODO: Nothing = scala.Predef.???

  implicit def monad: Monad[F]
  implicit def booleanAlgebra: BooleanAlgebra[Rep]
  implicit def numericAlgebra: NumericAlgebra[Rep]
  implicit def orderAlgebra: OrderAlgebra[Rep]

  // implicit def booleanAlgebraF: BooleanAlgebra[F[Rep]] = BooleanAlgebra.lift[F, Rep](booleanAlgebra)
  // implicit def numericAlgebraF: NumericAlgebra[F[Rep]] = NumericAlgebra.lift[F, Rep](numericAlgebra)
  // implicit def orderAlgebraF: OrderAlgebra[F[Rep]]     = OrderAlgebra.lift[F, Rep](orderAlgebra)

  type QsAlgebra[QS[_]]   = AlgebraM[F, QS, Rep]
  type QsExtractor[QS[_]] = Extractor[QS[Rep], F[Rep]]

  def queryFile: QueryFile ~> F
  def readFile: ReadFile ~> F
  def writeFile: WriteFile ~> F
  def manageFile: ManageFile ~> F
  def fileSystem: FileSystem ~> F

  def undefined: Rep
  def isUndefined(value: Rep): Boolean
  def hasType(scrutinee: Rep, tpe: Type): Boolean
  def constant(literal: T[EJson]): Rep
  def asBoolean(value: Rep): F[Boolean]

  val MF: MapFuncExtractors

  trait MapFuncExtractors {
    type Ex = QsExtractor[MapFunc]

    val Time: Ex
    val Math: Ex
    val Bool: Ex
    val Str: Ex
    val Structural: Ex
    val Special: Ex
  }

  def mapFunc: QsAlgebra[MapFunc] = {
    case MF.Time(x)       => x
    case MF.Math(x)       => x
    case MF.Bool(x)       => x
    case MF.Str(x)        => x
    case MF.Structural(x) => x
    case MF.Special(x)    => x
  }
}

trait FreshImpls[T[_[_]], F[_], Rep] extends Fresh[T, F, Rep] {
  self =>

  private implicit def liftRep(x: Rep): F[Rep] = x.point[F]
  import BooleanAlgebra._, NumericAlgebra._, OrderAlgebra._

  object MF extends MapFuncExtractors {
    def mk(pf: PartialFunction[MapFunc[Rep], F[Rep]]) = Extractor partial pf

    val Time = mk {
      case mf.Date(s)                     => TODO
      case mf.Interval(s)                 => TODO
      case mf.Length(len)                 => TODO
      case mf.Now()                       => TODO
      case mf.Time(s)                     => TODO
      case mf.TimeOfDay(dt)               => TODO
      case mf.Timestamp(s)                => TODO
      case mf.ToTimestamp(millis)         => TODO
      case mf.ExtractCentury(time)        => TODO
      case mf.ExtractDayOfMonth(time)     => TODO
      case mf.ExtractDayOfWeek(time)      => TODO
      case mf.ExtractDayOfYear(time)      => TODO
      case mf.ExtractDecade(time)         => TODO
      case mf.ExtractEpoch(time)          => TODO
      case mf.ExtractHour(time)           => TODO
      case mf.ExtractIsoDayOfWeek(time)   => TODO
      case mf.ExtractIsoYear(year)        => TODO
      case mf.ExtractMicroseconds(time)   => TODO
      case mf.ExtractMillennium(time)     => TODO
      case mf.ExtractMilliseconds(time)   => TODO
      case mf.ExtractMinute(time)         => TODO
      case mf.ExtractMonth(time)          => TODO
      case mf.ExtractQuarter(time)        => TODO
      case mf.ExtractSecond(time)         => TODO
      case mf.ExtractTimezone(time)       => TODO
      case mf.ExtractTimezoneHour(time)   => TODO
      case mf.ExtractTimezoneMinute(time) => TODO
      case mf.ExtractWeek(time)           => TODO
      case mf.ExtractYear(time)           => TODO
    }
    val Math = mk {
      case mf.Negate(x)      => -x
      case mf.Add(x, y)      => x + y
      case mf.Multiply(x, y) => x * y
      case mf.Subtract(x, y) => x - y
      case mf.Divide(x, y)   => x / y
      case mf.Modulo(x, y)   => x % y
      case mf.Power(b, e)    => b ** e
    }
    val Bool = mk {
      case mf.Not(x)             => !x
      case mf.Eq(x, y)           => x === y
      case mf.Neq(x, y)          => x =/= y
      case mf.Lt(x, y)           => x < y
      case mf.Lte(x, y)          => x <= y
      case mf.Gt(x, y)           => x > y
      case mf.Gte(x, y)          => x >= y
      case mf.And(x, y)          => x && y
      case mf.Or(x, y)           => x || y
      case mf.Between(x, lo, hi) => lo <= x && x <= hi
      case mf.Within(item, arr)  => TODO
    }
    val Str = mk {
      case mf.Lower(s)                        => TODO
      case mf.Upper(s)                        => TODO
      case mf.Bool(s)                         => TODO
      case mf.Integer(s)                      => TODO
      case mf.Decimal(s)                      => TODO
      case mf.Null(s)                         => TODO
      case mf.ToString(value)                 => TODO
      case mf.Search(s, pattern, insensitive) => TODO
      case mf.Substring(s, offset, length)    => TODO
    }
    val Structural = mk {
      case mf.ConcatArrays(xs, ys)     => TODO
      case mf.ConcatMaps(xs, ys)       => TODO
      case mf.DeleteField(src, field)  => TODO
      case mf.DupArrayIndices(arr)     => TODO
      case mf.DupMapKeys(map)          => TODO
      case mf.MakeArray(arr)           => TODO
      case mf.MakeMap(key, value)      => TODO
      case mf.ProjectField(src, field) => TODO
      case mf.ProjectIndex(arr, idx)   => TODO
      case mf.Range(from, to)          => TODO
      case mf.ZipArrayIndices(arr)     => TODO
      case mf.ZipMapKeys(map)          => TODO
    }
    val Special = mk {
      case mf.Cond(p, ifp, elsep)               => Monad[F].ifM(asBoolean(p), ifp, elsep)
      case mf.Constant(lit)                     => constant(lit)
      case mf.Guard(scrutinee, tpe, ifp, elsep) => if (hasType(scrutinee, tpe)) ifp else elsep
      case mf.IfUndefined(value, alt)           => if (isUndefined(value)) alt else value
      case mf.Undefined()                       => undefined
    }
  }
}
