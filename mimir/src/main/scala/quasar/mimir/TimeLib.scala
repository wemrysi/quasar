/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir

import java.time.format.DateTimeParseException

import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._
import java.time.{LocalDate => JLocalDate, LocalDateTime => JLocalDateTime, LocalTime => JLocalTime}
import java.time.{ZoneOffset, OffsetDateTime => JOffsetDateTime, OffsetTime => JOffsetTime}

import quasar.{DateTimeInterval, TemporalPart, datetime}
import quasar.yggdrasil.util.ColumnDateTimeExtractors._
import scalaz.syntax.show._

trait TimeLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait TimeLib extends ColumnarTableLib {

    val TimeNamespace = Vector("std", "time")

    override def _lib1 = super._lib1 ++ Set(
      OffsetDate,
      OffsetDateTime,
      OffsetTime,
      LocalDate,
      LocalDateTime,
      LocalTime,
      Duration,

      ExtractCentury,
      ExtractDayOfMonth,
      ExtractDecade,
      ExtractDayOfWeek,
      ExtractDayOfYear,
      ExtractHour,
      ExtractIsoDayOfWeek,
      ExtractIsoYear,
      ExtractMicrosecond,
      ExtractMillennium,
      ExtractMillisecond,
      ExtractMinute,
      ExtractMonth,
      ExtractQuarter,
      ExtractSecond,
      ExtractTimeZone,
      ExtractTimeZoneMinute,
      ExtractTimeZoneHour,
      ExtractWeek,
      ExtractYear,

      ExtractEpoch,

      TruncCentury,
      TruncDay,
      TruncDecade,
      TruncHour,
      TruncMicrosecond,
      TruncMillennium,
      TruncMillisecond,
      TruncMinute,
      TruncMonth,
      TruncQuarter,
      TruncSecond,
      TruncWeek,
      TruncYear)

    override def _lib2 = super._lib2

    val ExtractEpoch = new Op1F1(TimeNamespace, "extractEpoch") {
      val tpe = UnaryOperationType(JOffsetDateTimeT, JNumberT)
      def f1: F1 = CF1P("builtin::time::extractEpoch") {
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with DoubleColumn {
            def apply(row: Int) = datetime.extractEpoch(c(row))
          }
      }
    }

    final class DateLongExtractor(name: String, extract: JLocalDate => Long) extends Op1F1(TimeNamespace, "extract" + name) {
      val tpe = UnaryOperationType(JType.JDateT, JNumberT)
      def f1: F1 = CF1P("builtin::time::extract" + name) {
        case AsDateColumn(c) =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row))
          }
      }
    }

    final class TimeLongExtractor(name: String, extract: JLocalTime => Long) extends Op1F1(TimeNamespace, "extract" + name) {
      val tpe = UnaryOperationType(JType.JTimeT, JNumberT)
      def f1: F1 = CF1P("builtin::time::extract" + name) {
        case AsTimeColumn(c) =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row))
          }
      }
    }

    final class OffsetIntExtractor(name: String, extract: ZoneOffset => Int) extends Op1F1(TimeNamespace, "extract" + name) {
      val tpe = UnaryOperationType(JType.JOffsetT, JNumberT)
      def f1: F1 = CF1P("builtin::time::extract" + name) {
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row).getOffset).toLong
          }
        case c: OffsetDateColumn =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row).offset).toLong
          }
        case c: OffsetTimeColumn =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row).getOffset).toLong
          }
      }
    }

    final class OffsetIntSetter(name: String, create: Int => ZoneOffset, set: (Int, ZoneOffset) => ZoneOffset) extends Op2F2(TimeNamespace, "set" + name) {
      val tpe = BinaryOperationType(JNumberT, JType.JOffsetT, JType.JOffsetT)
      def f2: F2 = CF2P("builtin::time::extract" + name) {
        case (n: LongColumn, c: OffsetDateTimeColumn) =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              val r = c(row)
              r.withOffsetSameLocal(set(n(row).toInt, r.getOffset))
            }
          }
        case (n: LongColumn, c: OffsetDateColumn) =>
          new Map1Column(c) with OffsetDateColumn {
            def apply(row: Int) = {
              val r = c(row)
              r.copy(offset = set(n(row).toInt, r.offset))
            }
          }
        case (n: LongColumn, c: OffsetTimeColumn) =>
          new Map1Column(c) with OffsetTimeColumn {
            def apply(row: Int) = {
              val r = c(row)
              r.withOffsetSameLocal(set(n(row).toInt, r.getOffset))
            }
          }
        case (n: LongColumn, c: LocalDateTimeColumn) =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = JOffsetDateTime.of(c(row), create(n(row).toInt))
          }
        case (n: LongColumn, c: LocalDateColumn) =>
          new Map1Column(c) with OffsetDateColumn {
            def apply(row: Int) = quasar.OffsetDate(c(row), create(n(row).toInt))
          }
        case (n: LongColumn, c: LocalTimeColumn) =>
          new Map1Column(c) with OffsetTimeColumn {
            def apply(row: Int) = JOffsetTime.of(c(row), create(n(row).toInt))
          }
      }
    }

    val ExtractCentury = new DateLongExtractor("Century", datetime.extractCentury)
    val ExtractDayOfMonth = new DateLongExtractor("DayOfMonth", datetime.extractDayOfMonth)
    val ExtractDecade = new DateLongExtractor("Decade", datetime.extractDecade)
    val ExtractDayOfWeek = new DateLongExtractor("DayOfWeek", datetime.extractDayOfWeek)
    val ExtractDayOfYear = new DateLongExtractor("DayOfYear", datetime.extractDayOfYear)
    val ExtractIsoDayOfWeek = new DateLongExtractor("IsoDayOfWeek", datetime.extractIsoDayOfWeek)
    val ExtractIsoYear = new DateLongExtractor("IsoYear", datetime.extractIsoYear)
    val ExtractMillennium = new DateLongExtractor("Millennium", datetime.extractMillennium)
    val ExtractMonth = new DateLongExtractor("Month", datetime.extractMonth)
    val ExtractQuarter = new DateLongExtractor("Quarter", datetime.extractQuarter)
    val ExtractWeek = new DateLongExtractor("Week", datetime.extractWeek)
    val ExtractYear = new DateLongExtractor("Year", datetime.extractYear)

    val ExtractHour = new TimeLongExtractor("Hour", datetime.extractHour)
    val ExtractMicrosecond = new TimeLongExtractor("Microsecond", datetime.extractMicrosecond)
    val ExtractMillisecond = new TimeLongExtractor("Millisecond", datetime.extractMillisecond)
    val ExtractMinute = new TimeLongExtractor("Minute", datetime.extractMinute)
    val ExtractSecond = new Op1F1(TimeNamespace, "extractSecond") {
      val tpe = UnaryOperationType(JType.JOffsetT, JNumberT)
      def f1: F1 = CF1P("builtin::time::extractSecond") {
        case AsTimeColumn(c) =>
          new Map1Column(c) with NumColumn {
            def apply(row: Int) = datetime.extractSecond(c(row))
          }
      }
    }

    val ExtractTimeZone = new OffsetIntExtractor("TimeZone", datetime.extractTimeZone)
    val ExtractTimeZoneMinute = new OffsetIntExtractor("TimeZoneMinute", datetime.extractTimeZoneMinute)
    val ExtractTimeZoneHour = new OffsetIntExtractor("TimeZoneHour", datetime.extractTimeZoneHour)

    val SetTimeZone = new OffsetIntSetter("TimeZone",
      ZoneOffset.ofTotalSeconds,
      (i, _) => ZoneOffset.ofTotalSeconds(i))

    val SetTimeZoneMinute = new OffsetIntSetter("TimeZoneMinute",
      ZoneOffset.ofHoursMinutes(0, _),
      { (i, zo) =>
        val totalSeconds: Int = zo.getTotalSeconds
        val minuteField: Int = (totalSeconds % 3600) / 60
        ZoneOffset.ofTotalSeconds(totalSeconds - (minuteField * 60) + (i * 60))
      })

    val SetTimeZoneHour = new OffsetIntSetter("TimeZoneHour",
      ZoneOffset.ofHours,
      (i, zo) => ZoneOffset.ofTotalSeconds(i * 3600 + zo.getTotalSeconds % 3600))

    val LocalDate = new Op1F1(TimeNamespace, "localdate") {
      def f1: F1 = CF1P("builtin::time::localdate") {
        case c: StrColumn => new LocalDateColumn {
          def apply(row: Int) = JLocalDate.parse(c(row))
          def isDefinedAt(row: Int) = {
            try {
              JLocalDate.parse(c(row))
              true
            } catch { case (_: DateTimeParseException) =>
              false
            }
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalDateT)
    }

    val LocalDateTime = new Op1F1(TimeNamespace, "localdatetime") {
      def f1: F1 = CF1P("builtin::time::localdatetime") {
        case c: StrColumn => new LocalDateTimeColumn {
          def apply(row: Int) = JLocalDateTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            try {
              JLocalDateTime.parse(c(row))
              true
            } catch { case (_: DateTimeParseException) =>
              false
            }
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalDateTimeT)
    }

    val LocalTime = new Op1F1(TimeNamespace, "localtime") {
      def f1: F1 = CF1P("builtin::time::localtime") {
        case c: StrColumn => new LocalTimeColumn {
          def apply(row: Int) = JLocalTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            try {
              JLocalTime.parse(c(row))
              true
            } catch { case (_: DateTimeParseException) =>
              false
            }
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalTimeT)
    }

    val OffsetDate = new Op1F1(TimeNamespace, "offsetdate") {
      def f1: F1 = CF1P("builtin::time::offsetdate") {
        case c: StrColumn =>
          new OffsetDateColumn {
            def apply(row: Int) = quasar.OffsetDate.parse(c(row))
            def isDefinedAt(row: Int) = {
              try {
                quasar.OffsetDate.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            }
          }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetDateT)
    }

    val OffsetDateTime = new Op1F1(TimeNamespace, "offsetdatetime") {
      def f1: F1 = CF1P("builtin::time::offsetdatetime") {
        case c: StrColumn => new OffsetDateTimeColumn {
          def apply(row: Int) = JOffsetDateTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            try {
              JOffsetDateTime.parse(c(row))
              true
            } catch { case (_: DateTimeParseException) =>
              false
            }
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetDateTimeT)
    }

    val OffsetTime = new Op1F1(TimeNamespace, "offsettime") {
      def f1: F1 = CF1P("builtin::time::offsettime") {
        case c: StrColumn => new OffsetTimeColumn {
          def apply(row: Int) = JOffsetTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            try {
              JOffsetTime.parse(c(row))
              true
            } catch { case (_: DateTimeParseException) =>
              false
            }
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetTimeT)
    }

    val Duration = new Op1F1(TimeNamespace, "duration") {
      def f1: F1 = CF1P("builtin::time::duration") {
        case c: StrColumn => new DurationColumn {
          def apply(row: Int) = DateTimeInterval.parse(c(row)).get
          def isDefinedAt(row: Int) = DateTimeInterval.parse(c(row)).isDefined
        }
      }
      val tpe = UnaryOperationType(JTextT, JDurationT)
    }

    final case class Trunc(truncPart: TemporalPart) extends Op1F1(TimeNamespace, "trunc" + truncPart.shows) {
      def f1: F1 = CF1P("builtin::time::trunc" + truncPart.shows) {
        case c: OffsetDateTimeColumn => new Map1Column(c) with OffsetDateTimeColumn {
          def apply(row: Int) = {
            val r = c(row)
            JOffsetDateTime.of(datetime.truncDateTime(truncPart, r.toLocalDateTime), r.getOffset)
          }
        }
        case c: OffsetDateColumn => new Map1Column(c) with OffsetDateColumn {
          def apply(row: Int) = {
            val r = c(row)
            quasar.OffsetDate(datetime.truncDate(truncPart, r.date), r.offset)
          }
        }
        case c: OffsetTimeColumn => new Map1Column(c) with OffsetTimeColumn {
          def apply(row: Int) = {
            val r = c(row)
            JOffsetTime.of(datetime.truncTime(truncPart, r.toLocalTime), r.getOffset)
          }
        }
        case c: LocalDateTimeColumn => new Map1Column(c) with LocalDateTimeColumn {
          def apply(row: Int) = datetime.truncDateTime(truncPart, c(row))
        }
        case c: LocalDateColumn => new Map1Column(c) with LocalDateColumn {
          def apply(row: Int) = datetime.truncDate(truncPart, c(row))
        }
        case c: LocalTimeColumn => new Map1Column(c) with LocalTimeColumn {
          def apply(row: Int) = datetime.truncTime(truncPart, c(row))
        }
      }
      val tpe = UnaryOperationType(JType.JTemporalAbsoluteT, JType.JTemporalAbsoluteT)
    }

    def truncPart(part: TemporalPart): Trunc = part match {
      case TemporalPart.Century => TruncCentury
      case TemporalPart.Day => TruncDay
      case TemporalPart.Decade => TruncDecade
      case TemporalPart.Hour => TruncHour
      case TemporalPart.Microsecond => TruncMicrosecond
      case TemporalPart.Millennium => TruncMillennium
      case TemporalPart.Millisecond => TruncMillisecond
      case TemporalPart.Minute => TruncMinute
      case TemporalPart.Month => TruncMonth
      case TemporalPart.Quarter => TruncQuarter
      case TemporalPart.Second => TruncSecond
      case TemporalPart.Week => TruncWeek
      case TemporalPart.Year => TruncYear
    }

    val TruncCentury = Trunc(TemporalPart.Century)
    val TruncDay = Trunc(TemporalPart.Day)
    val TruncDecade = Trunc(TemporalPart.Decade)
    val TruncHour = Trunc(TemporalPart.Hour)
    val TruncMicrosecond = Trunc(TemporalPart.Microsecond)
    val TruncMillennium = Trunc(TemporalPart.Millennium)
    val TruncMillisecond = Trunc(TemporalPart.Millisecond)
    val TruncMinute = Trunc(TemporalPart.Minute)
    val TruncMonth = Trunc(TemporalPart.Month)
    val TruncQuarter = Trunc(TemporalPart.Quarter)
    val TruncSecond = Trunc(TemporalPart.Second)
    val TruncWeek = Trunc(TemporalPart.Week)
    val TruncYear = Trunc(TemporalPart.Year)

    val TimeOfDay = new Op1F1(TimeNamespace, "timeofDay") {
      def f1: F1 = CF1P("builtin::time::timeofday") {
        case c: LocalDateTimeColumn =>
          new Map1Column(c) with LocalTimeColumn {
            def apply(row: Int) = c(row).toLocalTime
          }
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with OffsetTimeColumn {
            def apply(row: Int) = c(row).toOffsetTime
          }
      }
      val tpe = UnaryOperationType(JOffsetDateTimeT | JLocalDateTimeT, JType.JTimeT)
    }

    val StartOfDay = new Op1F1(TimeNamespace, "startofday") {
      def f1: F1 = CF1P("builtin::time::startofday") {
        case c: LocalDateTimeColumn =>
          new Map1Column(c) with LocalDateTimeColumn {
            def apply(row: Int) = datetime.truncDateTime(TemporalPart.Day, c(row))
          }
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              val r = c(row)
              JOffsetDateTime.of(datetime.truncDateTime(TemporalPart.Day, r.toLocalDateTime), r.getOffset)
            }
          }
        case c: LocalDateColumn =>
          new Map1Column(c) with LocalDateTimeColumn {
            def apply(row: Int) = JLocalDateTime.of(c(row), JLocalTime.MIN)
          }
        case c: OffsetDateColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              val r = c(row)
              JOffsetDateTime.of(r.date, JLocalTime.MIN, r.offset)
            }
          }
      }
      val tpe = UnaryOperationType(JType.JDateT, JType.JDateTimeT)
    }
  }
}
