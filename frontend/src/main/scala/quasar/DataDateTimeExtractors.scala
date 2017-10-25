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
import java.time.{LocalDate => JLocalDate, LocalDateTime => JLocalDateTime, LocalTime => JLocalTime}
import java.time.{OffsetDateTime => JOffsetDateTime, ZoneOffset}
import scalaz._
import Data.{OffsetDate => DOffsetDate, _}

object DataDateTimeExtractors {

  object CanAddTime {
    def unapply(data: Data): Option[JLocalTime => Data] = data match {
      case LocalDateTime(v) => Some(
        jlt => LocalDateTime(JLocalDateTime.of(v.toLocalDate, jlt)))
      case LocalDate(v) => Some(
        jlt => LocalDateTime(JLocalDateTime.of(v, jlt)))
      case OffsetDateTime(v) => Some(
        jlt => OffsetDateTime(JOffsetDateTime.of(v.toLocalDate, jlt, v.getOffset)))
      case DOffsetDate(v) => Some(
        jlt => OffsetDateTime(JOffsetDateTime.of(v.date, jlt, v.offset)))
      case _ => None
    }
  }

  object CanLensTime {
    def unapply(data: Data): Option[Store[JLocalTime, Data]] = data match {
      case d@OffsetDateTime(_) => Some(datetime.lensTimeOffsetDateTime(d.value).map(OffsetDateTime))
      case d@OffsetTime(_) => Some(datetime.lensTimeOffsetTime(d.value).map(OffsetTime))
      case d@LocalDateTime(_) => Some(datetime.lensTimeLocalDateTime(d.value).map(LocalDateTime))
      case d@LocalTime(_) => Some(Store(LocalTime, d.value))
      case _ => None
    }
  }

  object CanLensDate {
    def unapply(data: Data): Option[Store[JLocalDate, Data]] = data match {
      case d@OffsetDateTime(_) => Some(datetime.lensDateOffsetDateTime(d.value).map(OffsetDateTime))
      case d@DOffsetDate(_) => Some(datetime.lensDateOffsetDate(d.value).map(DOffsetDate))
      case d@LocalDateTime(_) => Some(datetime.lensDateLocalDateTime(d.value).map(LocalDateTime))
      case d@LocalDate(_) => Some(Store(LocalDate, d.value))
      case _ => None
    }
  }

  object CanLensDateTime {
    def unapply(data: Data): Option[Store[JLocalDateTime, Data]] = data match {
      case d@OffsetDateTime(_) => Some(datetime.lensDateTimeOffsetDateTime(d.value).map(OffsetDateTime))
      case d@LocalDateTime(_) => Some(datetime.lensDateTimeLocalDateTime(d.value).map(LocalDateTime))
      case _ => None
    }
  }

  object CanLensTimezone {
    def unapply(data: Data): Option[Store[ZoneOffset, Data]] = data match {
      case d@OffsetDateTime(_) => Some(datetime.lensTimeZoneOffsetDateTime(d.value).map(OffsetDateTime))
      case d@DOffsetDate(_) => Some(datetime.lensTimeZoneOffsetDate(d.value).map(DOffsetDate))
      case d@OffsetTime(_) => Some(datetime.lensTimeZoneOffsetTime(d.value).map(OffsetTime))
      case _ => None
    }
  }

  object CanSetTimezone {
    def unapply(data: Data): Option[ZoneOffset => Data] = data match {
      case d@OffsetDateTime(_) => Some(zo => OffsetDateTime(JOffsetDateTime.of(d.value.toLocalDateTime, zo)))
      case d@DOffsetDate(_) => Some(zo => DOffsetDate(OffsetDate(d.value.date, zo)))
      case d@LocalDateTime(_) => Some(zo => OffsetDateTime(JOffsetDateTime.of(d.value, zo)))
      case d@LocalDate(_) => Some(zo => DOffsetDate(OffsetDate(d.value, zo)))
      case _ => None
    }
  }

  object CanRemoveTime {
    def unapply(data: Data): Option[Data] = data match {
      case LocalDateTime(v) => Some(LocalDate(v.toLocalDate))
      case OffsetDateTime(v) => Some(DOffsetDate(OffsetDate(v.toLocalDate, v.getOffset)))
      case _ => None
    }
  }
}

