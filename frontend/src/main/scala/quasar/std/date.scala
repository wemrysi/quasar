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

package quasar.std

import quasar.Predef._
import quasar.{Data, Func, UnaryFunc, GenericFunc, Mapping, Type, SemanticError}, SemanticError._
import quasar.fp.ski._

import org.threeten.bp.{Duration, Instant, LocalDate, LocalTime, Period, ZoneOffset}
import scalaz._, Validation.success
import shapeless.{Data => _, _}

trait DateLib extends Library {
  def parseTimestamp(str: String): SemanticError \/ Data.Timestamp =
    \/.fromTryCatchNonFatal(Instant.parse(str)).bimap(
      κ(DateFormatError(Timestamp, str, None)),
      Data.Timestamp.apply)

  def parseDate(str: String): SemanticError \/ Data.Date =
    \/.fromTryCatchNonFatal(LocalDate.parse(str)).bimap(
      κ(DateFormatError(Date, str, None)),
      Data.Date.apply)

  def parseTime(str: String): SemanticError \/ Data.Time =
    \/.fromTryCatchNonFatal(LocalTime.parse(str)).bimap(
      κ(DateFormatError(Time, str, None)),
      Data.Time.apply)

  def parseInterval(str: String): SemanticError \/ Data.Interval = {
    \/.fromTryCatchNonFatal(Duration.parse(str)).bimap(
      κ(DateFormatError(Interval, str, Some("expected, e.g. P3DT12H30M15.0S; note: year/month not currently supported"))),
      Data.Interval.apply)
  }

  def startOfDayInstant(date: LocalDate): Instant =
    date.atStartOfDay.atZone(ZoneOffset.UTC).toInstant

  def startOfDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value))

  def startOfNextDay(date: Data.Date): Data.Timestamp =
    Data.Timestamp(startOfDayInstant(date.value.plus(Period.ofDays(1))))

  // NB: SQL specifies a function called `extract`, but that doesn't have comma-
  //     separated arguments. `date_part` is Postgres’ name for the same thing
  //     with commas.

  private def extract(name: String, help: String) =
    UnaryFunc(
      Mapping, name, help,
      Type.Numeric,
      Func.Input1(Type.Temporal),
      noSimplification,
      constTyper[nat._1](Type.Numeric),
      basicUntyper)

  val ExtractCentury      = extract("extract_century",
    "Pulls out the century subfield from a date/time value (currently year/100).")
  val ExtractDayOfMonth   = extract("extract_day_of_month",
    "Pulls out the day of month (`day`) subfield from a date/time value (1-31).")
  val ExtractDecade       = extract("extract_decade",
    "Pulls out the decade subfield from a date/time value (year/10).")
  val ExtractDayOfWeek    = extract("extract_day_of_week",
    "Pulls out the day of week (`dow`) subfield from a date/time value " +
    "(Sunday: 0 to Saturday: 7).")
  val ExtractDayOfYear    = extract("extract_day_of_year",
    "Pulls out the day of year (`doy`) subfield from a date/time value (1-365 or -366).")
  val ExtractEpoch        = extract("extract_epoch",
    "Pulls out the epoch subfield from a date/time value. For dates and " +
    "timestamps, this is the number of seconds since midnight, 1970-01-01. " +
    "For intervals, the number of seconds in the interval.")
  val ExtractHour         = extract("extract_hour",
    "Pulls out the hour subfield from a date/time value (0-23).")
  val ExtractIsoDayOfWeek       = extract("extract_iso_day_of_week",
    "Pulls out the ISO day of week (`isodow`) subfield from a date/time value " +
    "(Monday: 1 to Sunday: 7).")
  val ExtractIsoYear      = extract("extract_iso_year",
    "Pulls out the ISO year (`isoyear`) subfield from a date/time value (based " +
    "on the first week containing Jan. 4).")
  val ExtractMicroseconds = extract("extract_microseconds",
    "Pulls out the microseconds subfield from a date/time value (including seconds).")
  val ExtractMillennium    = extract("extract_millennium",
    "Pulls out the millennium subfield from a date/time value (currently year/1000).")
  val ExtractMilliseconds = extract("extract_milliseconds",
    "Pulls out the milliseconds subfield from a date/time value (including seconds).")
  val ExtractMinute       = extract("extract_minute",
    "Pulls out the minute subfield from a date/time value (0-59).")
  val ExtractMonth        = extract("extract_month",
    "Pulls out the month subfield from a date/time value (1-12).")
  val ExtractQuarter      = extract("extract_quarter",
    "Pulls out the quarter subfield from a date/time value (1-4).")
  val ExtractSecond = extract("extract_second",
    "Pulls out the second subfield from a date/time value (0-59, with fractional parts).")
  val ExtractTimezone = extract("extract_timezone",
    "Pulls out the timezone subfield from a date/time value (in seconds east of UTC).")
  val ExtractTimezoneHour = extract("extract_timezone_hour",
    "Pulls out the hour component of the timezone subfield from a date/time value.")
  val ExtractTimezoneMinute = extract("extract_timezone_minute",
    "Pulls out the minute component of the timezone subfield from a date/time value.")
  val ExtractWeek = extract("extract_week",
    "Pulls out the week subfield from a date/time value (1-53).")
  val ExtractYear = extract("extract_year",
    "Pulls out the year subfield from a date/time value.")

  val Date = UnaryFunc(
    Mapping,
    "date",
    "Converts a string in the format (YYYY-MM-DD) to a date value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Date,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseDate(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.Date)
    },
    basicUntyper)

  val Time = UnaryFunc(
    Mapping,
    "time",
    "Converts a string in the format (HH:MM:SS[.SSS]) to a time value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Time,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseTime(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str)                  => success(Type.Time)
    },
    basicUntyper)

  val Timestamp = UnaryFunc(
    Mapping,
    "timestamp",
    "Converts a string in the format (ISO 8601, UTC, e.g. 2015-05-12T12:22:00Z) to a timestamp value. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Timestamp,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseTimestamp(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Timestamp)
    },
    basicUntyper)

  val Interval = UnaryFunc(
    Mapping,
    "interval",
    "Converts a string in the format (ISO 8601, e.g. P3DT12H30M15.0S) to an interval value. Note: year/month not currently supported. This is a partial function – arguments that don’t satisify the constraint have undefined results.",
    Type.Interval,
    Func.Input1(Type.Str),
    noSimplification,
    partialTyperV[nat._1] {
      case Sized(Type.Const(Data.Str(str))) => parseInterval(str).map(Type.Const(_)).validation.toValidationNel
      case Sized(Type.Str) => success(Type.Interval)
    },
    basicUntyper)

  val TimeOfDay = UnaryFunc(
    Mapping,
    "time_of_day",
    "Extracts the time of day from a (UTC) timestamp value.",
    Type.Time,
    Func.Input1(Type.Timestamp),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Timestamp(value))) => Type.Const(Data.Time(value.atZone(ZoneOffset.UTC).toLocalTime))
      case Sized(Type.Timestamp) => Type.Time
    },
    basicUntyper)

  val ToTimestamp = UnaryFunc(
    Mapping,
    "to_timestamp",
    "Converts an integer epoch time value (i.e. milliseconds since 1 Jan. 1970, UTC) to a timestamp constant.",
    Type.Timestamp,
    Func.Input1(Type.Int),
    noSimplification,
    partialTyper[nat._1] {
      case Sized(Type.Const(Data.Int(millis))) => Type.Const(Data.Timestamp(Instant.ofEpochMilli(millis.toLong)))
      case Sized(Type.Int) => Type.Timestamp
    },
    basicUntyper)

  def unaryFunctions: List[GenericFunc[nat._1]] =
    ExtractCentury :: ExtractDayOfMonth :: ExtractDecade :: ExtractDayOfWeek ::
    ExtractDayOfYear :: ExtractEpoch :: ExtractHour :: ExtractIsoDayOfWeek ::
    ExtractIsoYear :: ExtractMicroseconds :: ExtractMillennium ::
    ExtractMilliseconds :: ExtractMinute :: ExtractMonth :: ExtractQuarter ::
    ExtractSecond :: ExtractWeek :: ExtractYear ::
    Date :: Time :: Timestamp :: Interval :: TimeOfDay :: ToTimestamp :: Nil

  def binaryFunctions: List[GenericFunc[nat._2]] = Nil
  def ternaryFunctions: List[GenericFunc[nat._3]] = Nil
}

object DateLib extends DateLib
