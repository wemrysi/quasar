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

package quasar

import java.time._
import java.time.temporal.{ChronoField, ChronoUnit}

import slamdata.Predef._
import scalaz._
import Scalaz._

sealed abstract class TemporalPart extends Serializable

object TemporalPart {
  final case object Century     extends TemporalPart
  final case object Day         extends TemporalPart
  final case object Decade      extends TemporalPart
  final case object Hour        extends TemporalPart
  final case object Microsecond extends TemporalPart
  final case object Millennium  extends TemporalPart
  final case object Millisecond extends TemporalPart
  final case object Minute      extends TemporalPart
  final case object Month       extends TemporalPart
  final case object Quarter     extends TemporalPart
  final case object Second      extends TemporalPart
  final case object Week        extends TemporalPart
  final case object Year        extends TemporalPart

  implicit val equal: Equal[TemporalPart] = Equal.equalRef
  implicit val show: Show[TemporalPart] = Show.showFromToString
}

object datetime {
  import TemporalPart._
  type LensDate[I] = Lens[I, LocalDate]
  type SetDate[I, O] = (I, LocalDate) => O
  type LensTime[I] = Lens[I, LocalTime]
  type SetTime[I, O] = (I, LocalTime) => O
  type LensTimeZone[I] = Lens[I, ZoneOffset]
  type SetTimeZone[I, O] = (I, ZoneOffset) => O
  type LensDateTime[I] = Lens[I, LocalDateTime]
  type SetDateTime[I, O] = (I, LocalDateTime) => O

  def toLensDateTime[I](ld: LensDate[I], lt: LensTime[I]): LensDateTime[I] = Lens.lensu[I, LocalDateTime](
    (i, ldt) => ld.set(lt.set(i, ldt.toLocalTime), ldt.toLocalDate),
    i => LocalDateTime.of(ld.get(i), lt.get(i))
  )

  def toLensDate[I](ldt: LensDateTime[I]): LensDate[I] = ldt andThen lensDateLocalDateTime

  def toLensTime[I](ldt: LensDateTime[I]): LensTime[I] = ldt andThen lensTimeLocalDateTime

  val lensDateLocalDate: LensDate[LocalDate] = Lens.lensId
  val lensDateLocalDateTime: LensDate[LocalDateTime] =
    Lens.lensu[LocalDateTime, LocalDate]((ldt, ld) => LocalDateTime.of(ld, ldt.toLocalTime), _.toLocalDate)
  val lensDateOffsetDate: LensDate[OffsetDate] =
    Lens.lensu[OffsetDate, LocalDate]((od, ld) => OffsetDate(ld, od.offset), _.date)
  val lensDateOffsetDateTime: LensDate[OffsetDateTime] =
    Lens.lensu[OffsetDateTime, LocalDate]((odt, ld) => OffsetDateTime.of(ld, odt.toLocalTime, odt.getOffset), _.toLocalDate)

  val lensTimeLocalDateTime: LensTime[LocalDateTime] =
    Lens.lensu[LocalDateTime, LocalTime]((ldt, lt) => LocalDateTime.of(ldt.toLocalDate, lt), _.toLocalTime)
  val lensTimeLocalTime: LensTime[LocalTime] = Lens.lensId
  val lensTimeOffsetDateTime: LensTime[OffsetDateTime] =
    Lens.lensu[OffsetDateTime, LocalTime]((odt, lt) => OffsetDateTime.of(odt.toLocalDate, lt, odt.getOffset), _.toLocalTime)
  val lensTimeOffsetTime: LensTime[OffsetTime] =
    Lens.lensu[OffsetTime, LocalTime]((ot, lt) => OffsetTime.of(lt, ot.getOffset), _.toLocalTime)

  val lensDateTimeLocalDateTime = Lens.lensId[LocalDateTime]
  val lensDateTimeOffsetDateTime = toLensDateTime(lensDateOffsetDateTime, lensTimeOffsetDateTime)

  val setDateLocalTime: SetDate[LocalTime, LocalDateTime] =
    (lt, ld) => LocalDateTime.of(ld, lt)
  val setTimeLocalDate: SetTime[LocalDate, LocalDateTime] =
    (ld, lt) => LocalDateTime.of(ld, lt)

  val setDateOffsetTime: SetDate[OffsetTime, OffsetDateTime] =
    (ot, ld) => OffsetDateTime.of(ld, ot.toLocalTime, ot.getOffset)
  val setTimeOffsetDate: SetTime[OffsetDate, OffsetDateTime] =
    (od, lt) => OffsetDateTime.of(od.date, lt, od.offset)

  val lensTimeZoneOffsetDateTime: LensTimeZone[OffsetDateTime] =
    Lens.lensu[OffsetDateTime, ZoneOffset]((odt, o) => OffsetDateTime.of(odt.toLocalDateTime, o), _.getOffset)
  val lensTimeZoneOffsetTime: LensTimeZone[OffsetTime] =
    Lens.lensu[OffsetTime, ZoneOffset]((ot, o) => OffsetTime.of(ot.toLocalTime, o), _.getOffset)
  val lensTimeZoneOffsetDate: LensTimeZone[OffsetDate] =
    Lens.lensu[OffsetDate, ZoneOffset]((od, o) => OffsetDate(od.date, o), _.offset)

  val setTimeZoneLocalTime: SetTimeZone[LocalTime, OffsetTime] = OffsetTime.of
  val setTimeZoneLocalDate: SetTimeZone[LocalDate, OffsetDate] = OffsetDate(_, _)
  val setTimeZoneLocalDateTime: SetTimeZone[LocalDateTime, OffsetDateTime] = OffsetDateTime.of

  def truncHour(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.HOURS)
  def truncMicrosecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MICROS)
  def truncMillisecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MILLIS)
  def truncMinute(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.MINUTES)
  def truncSecond(i: LocalTime): LocalTime = i.truncatedTo(ChronoUnit.SECONDS)
  def truncCentury(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 100) * 100)
  def truncDecade(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 10) * 100)
  def truncMillennium(i: LocalDate): LocalDate =
    i.withDayOfMonth(1)
      .withMonth(1)
      .withYear((i.getYear / 10) * 100)
  def truncMonth(i: LocalDate): LocalDate =
    i
      .withDayOfMonth(1)
  def truncQuarter(i: LocalDate): LocalDate =
    i
      .withDayOfMonth(1)
      .withMonth(i.getMonth.firstMonthOfQuarter().getValue)
  def truncWeek(i: LocalDate): LocalDate =
    i
      .`with`(java.time.DayOfWeek.MONDAY)
  def truncYear(i: LocalDate): LocalDate =
    i
      .withDayOfMonth(1)
      .withMonth(1)

  def truncTime(part: TemporalPart, i: LocalTime): LocalTime =
      part match {
        case Hour        => truncHour(i)
        case Microsecond => truncMicrosecond(i)
        case Millisecond => truncMillisecond(i)
        case Minute      => truncMinute(i)
        case Second      => truncSecond(i)
        case _           => i
      }

  def truncDateTime(part: TemporalPart, i: LocalDateTime): LocalDateTime =
    if (part === Day) LocalDateTime.of(i.toLocalDate, LocalTime.MIN)
    else LocalDateTime.of(truncDate(part, i.toLocalDate), truncTime(part, i.toLocalTime))

  def truncDate(part: TemporalPart, date: LocalDate): LocalDate =
    part match {
      case Century    => truncCentury(date)
      case Decade     => truncDecade(date)
      case Millennium => truncMillennium(date)
      case Month      => truncMonth(date)
      case Quarter    => truncQuarter(date)
      case Week       => truncWeek(date)
      case Year       => truncYear(date)
      case _          =>
        date
    }

  def extractCentury(ld: LocalDate): Long = (ld.getLong(ChronoField.YEAR_OF_ERA) - 1) / 100 + 1
  def extractDayOfMonth(ld: LocalDate): Long = ld.getLong(ChronoField.DAY_OF_MONTH)
  def extractDecade(ld: LocalDate): Long = ld.getLong(ChronoField.YEAR_OF_ERA) / 10
  def extractDayOfYear(ld: LocalDate): Long = ld.getLong(ChronoField.DAY_OF_YEAR)
  def extractEpoch(odt: OffsetDateTime): Double = odt.toEpochSecond + odt.getNano / 1000000000.0
  def extractMillennium(ld: LocalDate): Long = (ld.getLong(ChronoField.YEAR) - 1) / 1000 + 1
  def extractMonth(ld: LocalDate): Int = ld.getMonthValue
  def extractQuarter(ld: LocalDate): Long = java.lang.Math.ceil(ld.getMonthValue / 3.0).toLong
  def extractWeek(ld: LocalDate): Long = if (ld.getDayOfYear === 1) 53 else ld.getLong(ChronoField.ALIGNED_WEEK_OF_YEAR)
  def extractYear(ld: LocalDate): Int = ld.getYear
  def extractIsoYear(ld: LocalDate): Long =
    // TODO: Come back to this, add tests for BC era
    if (ld.getDayOfYear === 1) ld.getLong(ChronoField.YEAR_OF_ERA) - 1
    else ld.getLong(ChronoField.YEAR_OF_ERA)
  def extractIsoDayOfWeek(ld: LocalDate): Int =
    ld.getDayOfWeek.getValue
  def extractDayOfWeek(ld: LocalDate): Int =
    ld.getDayOfWeek.getValue % 7
  def extractHour(lt: LocalTime): Int =
    lt.getHour
  def extractMicrosecond(lt: LocalTime): Long =
    lt.getLong(ChronoField.MICRO_OF_SECOND) + lt.getSecond * 1000000
  def extractMillisecond(lt: LocalTime): Long =
    lt.getLong(ChronoField.MILLI_OF_SECOND) + lt.getSecond * 1000
  def extractMinute(lt: LocalTime): Long =
    lt.getLong(ChronoField.MINUTE_OF_HOUR)
  def extractSecond(lt: LocalTime): BigDecimal =
    BigDecimal(lt.getNano) / 1000000000.0 + lt.getSecond
  def extractTimeZone(zo: ZoneOffset): Int =
    zo.getTotalSeconds
  def extractTimeZoneHour(zo: ZoneOffset): Int =
    zo.getTotalSeconds / 3600
  def extractTimeZoneMinute(zo: ZoneOffset): Int =
    (zo.getTotalSeconds / 60) % 60
}
