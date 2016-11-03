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

package ygg.pkg

import ygg._, common._
import java.time.ZoneOffset.UTC

trait PackageTime {
  type Instant  = java.time.Instant
  type Period   = java.time.Period
  type Duration = java.time.Duration
  type DateTime = java.time.LocalDateTime

  implicit class QuasarDurationOps(private val x: Duration) {
    def getMillis: Long = x.toMillis
  }
  implicit class QuasarPeriodOps(private val x: Period) {
    def getMillis: Long      = toDuration.getMillis
    def toDuration: Duration = java.time.Duration from x
  }
  implicit class QuasarDateTimeOps(private val x: DateTime) {
    def toUtcInstant: Instant        = x toInstant UTC
    def getMillis: Long              = toUtcInstant.toEpochMilli
  }

  object duration {
    def fromMillis(ms: Long): Duration = java.time.Duration ofMillis ms
  }
  object period {
    def fromMillis(ms: Long): Period = java.time.Period from (duration fromMillis ms)
  }
  object instant {
    def fromMillis(ms: Long): Instant             = java.time.Instant.ofEpochMilli(ms)
  }
  object dateTime {
    def fromMillis(ms: Long): DateTime = java.time.LocalDateTime.ofInstant(instant fromMillis ms, UTC)
  }
}
