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
  def one: A
  def zero: A
  def complement(a: A): A
  def and(a: A, b: A): A
  def or(a: A, b: A): A
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

object NumericAlgebra {
  def apply[A](implicit z: NumericAlgebra[A]): NumericAlgebra[A] = z
}
object BooleanAlgebra {
  def apply[A](implicit z: BooleanAlgebra[A]): BooleanAlgebra[A] = z
}
