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

import slamdata.Predef._
import quasar.pkg.tests._
import java.time._
import java.time.temporal.ChronoField

trait DateGenerators {
  implicit val arbDuration: Arbitrary[DateTimeInterval] = genInterval
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary((genEpochSeconds, genNanos) >> Instant.ofEpochSecond _)
  implicit val arbDate: Arbitrary[LocalDate]         = genLocalDate
  implicit val arbTime: Arbitrary[LocalTime]         = genLocalTime
  implicit val arbDateTime: Arbitrary[LocalDateTime] = genLocalDateTime
  implicit val arbOffsetDate: Arbitrary[OffsetDate]         = genOffsetDate
  implicit val arbOffsetTime: Arbitrary[OffsetTime]         = genOffsetTime
  implicit val arbOffsetDateTime: Arbitrary[OffsetDateTime] = genOffsetDateTime

  private def genSeconds: Gen[Long]     = genInt ^^ (_.toLong)
  private def genSecondOfDay: Gen[Long] = choose(0L, 24L * 60 * 60 - 1)
  private def genMillis: Gen[Long]      = choose(0L, 999L)
  private def genNanos: Gen[Long]       = choose(0L, 999999999L)
  private def genYears: Gen[Int]        = choose(-9999999, 9999999)
  private def genMonths: Gen[Int]       = choose(-100, 100)
  private def genDays: Gen[Int]         = choose(-1000, 1000)
  private def genEpochSeconds: Gen[Long] =
    choose(ChronoField.INSTANT_SECONDS.range().getLargestMinimum + 1, ChronoField.INSTANT_SECONDS.range().getSmallestMaximum - 1)

  // these are adjusted so that LocalDate.ofEpochDay(minLocalEpochDay) - genYears can't go below the
  // minimum year of LocalDate, vice versa for maxLocalEpochDay
  val minLocalEpochDay: Long = 365L * -9999999
  val maxLocalEpochDay: Long = 365L * 9999999

  def genZoneOffset: Gen[ZoneOffset] =
    Gen.choose[Int](ZoneOffset.MIN.getTotalSeconds, ZoneOffset.MAX.getTotalSeconds).map(ZoneOffset.ofTotalSeconds)

  def genLocalTime: Gen[LocalTime] =
    Gen.choose[Long](ChronoField.NANO_OF_DAY.range().getLargestMinimum, ChronoField.NANO_OF_DAY.range().getMaximum).map(LocalTime.ofNanoOfDay)

  def genLocalDate: Gen[LocalDate] =
    Gen.choose[Long](minLocalEpochDay, maxLocalEpochDay).map(LocalDate.ofEpochDay)

  def genOffsetDateTime: Gen[OffsetDateTime] = for {
    datetime <- genLocalDateTime
    zo <- genZoneOffset
  } yield OffsetDateTime.of(datetime, zo)

  def genOffsetTime: Gen[OffsetTime] = for {
    time <- genLocalTime
    zo <- genZoneOffset
  } yield OffsetTime.of(time, zo)

  def genOffsetDate: Gen[OffsetDate] = for {
    date <- genLocalDate
    zo <- genZoneOffset
  } yield OffsetDate(date, zo)

  def genInterval: Gen[DateTimeInterval] =
    Gen.oneOf(genDateTimeInterval, genTimeInterval, genDateInterval)

  def genDateTimeInterval: Gen[DateTimeInterval] =
    for {
      years <- genYears
      months <- genMonths
      days <- genDays
      seconds <- arbitrary[Int]
      nanos <- Gen.choose(0L, 999999999L)
    } yield DateTimeInterval(years, months, days, seconds.toLong, nanos)

  def genDateInterval: Gen[DateTimeInterval] =
    for {
      years <- genYears
      months <- genMonths
      days <- genDays
    } yield DateTimeInterval(years, months, days, 0, 0)

  def genTimeInterval: Gen[DateTimeInterval] =
    for {
      seconds <- arbitrary[Int]
      nanos <- Gen.choose(0L, 999999999L)
    } yield DateTimeInterval(0, 0, 0, seconds.toLong, nanos)

  def genLocalDateTime: Gen[LocalDateTime] =
    for {
      time <- genLocalTime
      date <- genLocalDate
    } yield LocalDateTime.of(date, time)

}

object DateGenerators extends DateGenerators
