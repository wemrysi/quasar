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

import quasar.time
import quasar.time.TemporalPart
import qdata.time.{DateTimeInterval, OffsetDate => QOffsetDate}
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.table._
import quasar.yggdrasil.util.ColumnDateTimeExtractors._

import java.time.{
  LocalDate => JLocalDate,
  LocalDateTime => JLocalDateTime,
  LocalTime => JLocalTime,
  OffsetDateTime => JOffsetDateTime,
  OffsetTime => JOffsetTime,
  ZoneOffset
}
import java.time.format.DateTimeParseException

trait TimeLibModule extends ColumnarTableLibModule {
  trait TimeLib extends ColumnarTableLib {

    val ExtractEpoch = new Op1F1 {
      val tpe = UnaryOperationType(JOffsetDateTimeT, JNumberT)
      def f1: CF1 = CF1P {
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with DoubleColumn {
            def apply(row: Int) = time.extractEpoch(c(row))
          }
      }
    }

    final class DateLongExtractor(extract: JLocalDate => Long) extends Op1F1 {
      val tpe = UnaryOperationType(JType.JDateT, JNumberT)
      def f1: CF1 = CF1P {
        case AsDateColumn(c) =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row))
          }
      }
    }

    final class TimeLongExtractor(extract: JLocalTime => Long) extends Op1F1 {
      val tpe = UnaryOperationType(JType.JTimeT, JNumberT)
      def f1: CF1 = CF1P {
        case AsTimeColumn(c) =>
          new Map1Column(c) with LongColumn {
            def apply(row: Int) = extract(c(row))
          }
      }
    }

    final class OffsetIntExtractor(extract: ZoneOffset => Int) extends Op1F1 {
      val tpe = UnaryOperationType(JType.JOffsetT, JNumberT)
      def f1: CF1 = CF1P {
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

    final class OffsetIntSetter(create: Int => ZoneOffset, set: (Int, ZoneOffset) => ZoneOffset) extends Op2F2 {
      val tpe = BinaryOperationType(JNumberT, JType.JOffsetT, JType.JOffsetT)
      def f2: CF2 = CF2P {
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
            def apply(row: Int) = QOffsetDate(c(row), create(n(row).toInt))
          }
        case (n: LongColumn, c: LocalTimeColumn) =>
          new Map1Column(c) with OffsetTimeColumn {
            def apply(row: Int) = JOffsetTime.of(c(row), create(n(row).toInt))
          }
      }
    }

    val ExtractCentury = new DateLongExtractor(time.extractCentury)
    val ExtractDayOfMonth = new DateLongExtractor(time.extractDayOfMonth)
    val ExtractDecade = new DateLongExtractor(time.extractDecade)
    val ExtractDayOfWeek = new DateLongExtractor(time.extractDayOfWeek)
    val ExtractDayOfYear = new DateLongExtractor(time.extractDayOfYear)
    val ExtractIsoDayOfWeek = new DateLongExtractor(time.extractIsoDayOfWeek)
    val ExtractIsoYear = new DateLongExtractor(time.extractIsoYear)
    val ExtractMillennium = new DateLongExtractor(time.extractMillennium)
    val ExtractMonth = new DateLongExtractor(time.extractMonth)
    val ExtractQuarter = new DateLongExtractor(time.extractQuarter)
    val ExtractWeek = new DateLongExtractor(time.extractWeek)
    val ExtractYear = new DateLongExtractor(time.extractYear)

    val ExtractHour = new TimeLongExtractor(time.extractHour)
    val ExtractMicrosecond = new TimeLongExtractor(time.extractMicrosecond)
    val ExtractMillisecond = new TimeLongExtractor(time.extractMillisecond)
    val ExtractMinute = new TimeLongExtractor(time.extractMinute)

    val ExtractSecond = new Op1F1 {
      val tpe = UnaryOperationType(JType.JOffsetT, JNumberT)
      def f1: CF1 = CF1P {
        case AsTimeColumn(c) =>
          new Map1Column(c) with NumColumn {
            def apply(row: Int) = time.extractSecond(c(row))
          }
      }
    }

    val ExtractTimeZone = new OffsetIntExtractor(time.extractTimeZone)
    val ExtractTimeZoneMinute = new OffsetIntExtractor(time.extractTimeZoneMinute)
    val ExtractTimeZoneHour = new OffsetIntExtractor(time.extractTimeZoneHour)

    val SetTimeZone = new OffsetIntSetter(
      ZoneOffset.ofTotalSeconds,
      (i, _) => ZoneOffset.ofTotalSeconds(i))

    val SetTimeZoneMinute = new OffsetIntSetter(
      ZoneOffset.ofHoursMinutes(0, _),
      (i, zo) => time.setTimeZoneMinute(zo, i))

    val SetTimeZoneHour = new OffsetIntSetter(
      ZoneOffset.ofHours,
      (i, zo) => time.setTimeZoneHour(zo, i))

    val LocalDate = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new LocalDateColumn {
          def apply(row: Int) = JLocalDate.parse(c(row))
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              try {
                JLocalDate.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalDateT)
    }

    val LocalDateTime = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new LocalDateTimeColumn {
          def apply(row: Int) = JLocalDateTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              try {
                JLocalDateTime.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalDateTimeT)
    }

    val LocalTime = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new LocalTimeColumn {
          def apply(row: Int) = JLocalTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              try {
                JLocalTime.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JLocalTimeT)
    }

    val OffsetDate = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn =>
          new OffsetDateColumn {
            def apply(row: Int) = QOffsetDate.parse(c(row))
            def isDefinedAt(row: Int) = {
              if (c.isDefinedAt(row)) {
                try {
                  QOffsetDate.parse(c(row))
                  true
                } catch { case (_: DateTimeParseException) =>
                  false
                }
              } else false
            }
          }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetDateT)
    }

    val OffsetDateTime = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new OffsetDateTimeColumn {
          def apply(row: Int) = JOffsetDateTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              try {
                JOffsetDateTime.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetDateTimeT)
    }

    val OffsetTime = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new OffsetTimeColumn {
          def apply(row: Int) = JOffsetTime.parse(c(row))
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              try {
                JOffsetTime.parse(c(row))
                true
              } catch { case (_: DateTimeParseException) =>
                false
              }
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JOffsetTimeT)
    }

    val Interval = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: StrColumn => new IntervalColumn {
          def apply(row: Int) = DateTimeInterval.parse(c(row)).get
          def isDefinedAt(row: Int) = {
            if (c.isDefinedAt(row)) {
              DateTimeInterval.parse(c(row)).isDefined
            } else false
          }
        }
      }
      val tpe = UnaryOperationType(JTextT, JIntervalT)
    }

    final case class Trunc(truncPart: TemporalPart) extends Op1F1 {
      def f1: CF1 = CF1P {
        case c: OffsetDateTimeColumn => new Map1Column(c) with OffsetDateTimeColumn {
          def apply(row: Int) = {
            val r = c(row)
            JOffsetDateTime.of(time.truncDateTime(truncPart, r.toLocalDateTime), r.getOffset)
          }
        }
        case c: OffsetDateColumn => new Map1Column(c) with OffsetDateColumn {
          def apply(row: Int) = {
            val r = c(row)
            QOffsetDate(time.truncDate(truncPart, r.date), r.offset)
          }
        }
        case c: OffsetTimeColumn => new Map1Column(c) with OffsetTimeColumn {
          def apply(row: Int) = {
            val r = c(row)
            JOffsetTime.of(time.truncTime(truncPart, r.toLocalTime), r.getOffset)
          }
        }
        case c: LocalDateTimeColumn => new Map1Column(c) with LocalDateTimeColumn {
          def apply(row: Int) = time.truncDateTime(truncPart, c(row))
        }
        case c: LocalDateColumn => new Map1Column(c) with LocalDateColumn {
          def apply(row: Int) = time.truncDate(truncPart, c(row))
        }
        case c: LocalTimeColumn => new Map1Column(c) with LocalTimeColumn {
          def apply(row: Int) = time.truncTime(truncPart, c(row))
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

    val ToTimestamp = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: DoubleColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            override def isDefinedAt(row: Int) = {
              super.isDefinedAt(row) && {
                val d = c(row)
                d == d.longValue
              }
            }

            def apply(row: Int) = {
              time.epochMilliToOffsetDateTime(c(row).longValue)
            }
          }

        case c: LongColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              time.epochMilliToOffsetDateTime(c(row))
            }
          }

        case c: NumColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            override def isDefinedAt(row: Int) = {
              super.isDefinedAt(row) && c(row).isValidLong
            }

            def apply(row: Int) = {
              time.epochMilliToOffsetDateTime(c(row).longValue)
            }
          }
      }
      val tpe = UnaryOperationType(JNumberT, JOffsetDateTimeT)
    }

    val TimeOfDay = new Op1F1 {
      def f1: CF1 = CF1P {
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

    val StartOfDay = new Op1F1 {
      def f1: CF1 = CF1P {
        case c: LocalDateTimeColumn =>
          new Map1Column(c) with LocalDateTimeColumn {
            def apply(row: Int) = time.truncDateTime(TemporalPart.Day, c(row))
          }
        case c: OffsetDateTimeColumn =>
          new Map1Column(c) with OffsetDateTimeColumn {
            def apply(row: Int) = {
              val r = c(row)
              JOffsetDateTime.of(time.truncDateTime(TemporalPart.Day, r.toLocalDateTime), r.getOffset)
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
