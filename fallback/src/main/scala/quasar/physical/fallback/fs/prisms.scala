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

package quasar.physical.fallback.fs

import quasar.Predef._
import monocle._
import java.time._

trait Prisms[A] {
  val Bool: Prism[A, Boolean]
  val Int: Prism[A, BigInt]
  val Dec: Prism[A, BigDecimal]
  val Str: Prism[A, String]
  val Null: A

  def undef: A
  def now(): Instant

  // A date without a time-zone in the ISO-8601 calendar system, such as 2007-12-03.
  val LocalDate: Prism[A, LocalDate]

  // An instantaneous point on the time-line.
  // This might be used to record event time-stamps in the application.
  val Instant: Prism[A, Instant]

  // A time-based amount of time, such as '34.5 seconds'.
  val Duration: Prism[A, Duration]

  // A time without a time-zone in the ISO-8601 calendar system, such as 10:15:30.
  val LocalTime: Prism[A, LocalTime]

/***
  // A date-time without a time-zone in the ISO-8601 calendar system, such as 2007-12-03T10:15:30.
  val LocalDateTime: Prism[A, LocalDateTime]

  // A date-time with a time-zone in the ISO-8601 calendar system, such as 2007-12-03T10:15:30+01:00 Europe/Paris.
  val ZonedDateTime: Prism[A, ZonedDateTime]

  // A date-based amount of time in the ISO-8601 calendar system, such as '2 years, 3 months and 4 days'.
  val Period: Prism[A, Period]
***/
}
