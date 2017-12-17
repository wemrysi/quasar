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

package quasar.std

import slamdata.Predef._, BigDecimal.RoundingMode
import quasar.{Data, Qspec, Type}
import quasar.DataGenerators.{dataArbitrary => _, _}
import quasar.frontend.logicalplan._
import quasar.{DateGenerators, DateTimeInterval, OffsetDate => JOffsetDate, TemporalPart}
import quasar.datetime.{truncDateTime, truncDate, truncTime}

import java.time.{Instant, LocalDate => JLocalDate, LocalDateTime => JLocalDateTime, LocalTime => JLocalTime, ZoneOffset}
import java.time.{OffsetDateTime => JOffsetDateTime, OffsetTime => JOffsetTime}
import quasar.pkg.tests._
import scala.collection.Traversable
import scala.math.abs
import scala.util.matching.Regex

import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.execute.Result
import org.specs2.matcher.{Expectable, Matcher}
import org.specs2.specification.core.Fragment
import org.scalacheck.{Arbitrary, Gen}
import scalaz._, Scalaz._

/** Abstract spec for the standard library, intended to be implemented for each
  * library implementation, of which there are one or more per backend.
  */
abstract class StdLibSpec extends Qspec {
  def isPrintableAscii(c: Char): Boolean = c >= '\u0020' && c <= '\u007e'
  def isPrintableAscii(s: String): Boolean = s.forall(isPrintableAscii)

  def beCloseTo(expected: Data): Matcher[Data] = new Matcher[Data] {
    def isClose(x: BigDecimal, y: BigDecimal, err: Double): Boolean =
      x == y || ((x - y).abs/(y.abs max err)).toDouble < err

    def apply[S <: Data](s: Expectable[S]) = {
      val v = s.value
      (v, expected) match {
        case (Data.Number(x), Data.Number(exp)) =>
          result(isClose(x, exp, 1e-9),
            s"$x is a Number and matches $exp",
            s"$x is a Number but does not match $exp", s)
        case _ =>
          result(Equal[Data].equal(v, expected),
            s"$v matches $expected",
            s"$v does not match $expected", s)
      }
    }
  }

  def tests(runner: StdLibTestRunner): Fragment = {
    import runner._

    implicit val arbBigInt = Arbitrary[BigInt] { runner.intDomain }
    implicit val arbBigDecimal = Arbitrary[BigDecimal] { runner.decDomain }
    implicit val arbString = Arbitrary[String] { runner.stringDomain }
    implicit val arbDate = Arbitrary[JLocalDate] { runner.dateDomain }
    implicit val arbTime = Arbitrary[JLocalTime] { runner.timeDomain }
    implicit val arbDateTime = Arbitrary[JLocalDateTime] { (runner.dateDomain, runner.timeDomain) >> JLocalDateTime.of }
    implicit val arbData = Arbitrary[Data] {
      Gen.oneOf(
        runner.intDomain.map(Data.Int(_)),
        runner.decDomain.map(Data.Dec(_)),
        runner.stringDomain.map(Data.Str(_)))
    }
    implicit val arbOffsetDateTime = Arbitrary[JOffsetDateTime] { (arbDateTime.gen, runner.timezoneDomain) >> JOffsetDateTime.of }
    implicit val arbOffsetDate =
      Arbitrary[JOffsetDate] { (runner.dateDomain, runner.timezoneDomain) >> (JOffsetDate(_, _)) }
    implicit val arbOffsetTime =
      Arbitrary[JOffsetTime] { (runner.timeDomain, runner.timezoneDomain) >> JOffsetTime.of }
    implicit val arbInterval = Arbitrary[DateTimeInterval] { runner.intervalDomain }

    def commute(
        prg: (Fix[LogicalPlan], Fix[LogicalPlan]) => Fix[LogicalPlan],
        arg1: Data, arg2: Data,
        expected: Data): Result =
      binary(prg, arg1, arg2, expected) and
        binary(prg, arg2, arg1, expected)

    "StringLib" >> {
      import StringLib._

      "Concat" >> {
        "any strings" >> prop { (str1: String, str2: String) =>
          binary(Concat(_, _).embed, Data.Str(str1), Data.Str(str2), Data.Str(str1 + str2))
        }
      }

      // NB: `Like` is simplified to `Search`

      "Search" >> {
        "find contents within string when case sensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("church"), Data.Str(".*ch.*"), Data.Bool(false), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("China"), Data.Str("^Ch.*$"), Data.Bool(false), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*ch.*"), Data.Bool(false), Data.Bool(true))
        }

        "reject a non-matching string when case sensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("church"), Data.Str("^bs.*$"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("china"), Data.Str("^bs.*$"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*bs.*"), Data.Bool(false), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matching"), Data.Str(".*CH.*"), Data.Bool(false), Data.Bool(false))
        }

        "find contents within string when case insensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("Church"), Data.Str(".*ch.*"), Data.Bool(true), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("cHina"), Data.Str("^ch.*$"), Data.Bool(true), Data.Bool(true)) and
            ternary(Search(_, _, _).embed, Data.Str("matCHing"), Data.Str(".*ch.*"), Data.Bool(true), Data.Bool(true))
        }

        "reject a non-matching string when case insensitive" >> {
          ternary(Search(_, _, _).embed, Data.Str("Church"), Data.Str("^bs.*$"), Data.Bool(true), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("cHina"), Data.Str("^bs.*$"), Data.Bool(true), Data.Bool(false)) and
            ternary(Search(_, _, _).embed, Data.Str("matCHing"), Data.Str(".*bs.*"), Data.Bool(true), Data.Bool(false))
        }
      }

      "Length" >> {
        "multibyte chars" >> {
          unary(Length(_).embed, Data.Str("€1"), Data.Int(2))
        }
        "any string" >> prop { (str: String) =>
          unary(Length(_).embed, Data.Str(str), Data.Int(str.length))
        }
      }

      "Lower" >> {
        "any string" >> prop { (str: String) =>
          unary(Lower(_).embed, Data.Str(str), Data.Str(str.toLowerCase))
        }
      }

      "Upper" >> {
        "any string" >> prop { (str: String) =>
          unary(Upper(_).embed, Data.Str(str), Data.Str(str.toUpperCase))
        }
      }

      "Substring" >> {
        "simple" >> {
          // NB: not consistent with PostgreSQL, which is 1-based for `start`
          ternary(Substring(_, _, _).embed, Data.Str("Thomas"), Data.Int(1), Data.Int(3), Data.Str("hom"))
        }

        "multibyte chars" >> {
          ternary(Substring(_, _, _).embed, Data.Str("cafétéria"), Data.Int(3), Data.Int(1), Data.Str("é"))
        }

        "empty string and any offsets" >> prop { (start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          val length = length0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(""), Data.Int(start), Data.Int(length), Data.Str(""))
        }

        "any string with entire range" >> prop { (str: String) =>
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(0), Data.Int(str.length), Data.Str(str))
        }

        "any string with 0 length" >> prop { (str: String, start0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(0), Data.Str(""))
        }

        "any string and offsets" >> prop { (str: String, start0: Int, length0: Int) =>
          // restrict the range to something that will actually exercise the behavior
          val start = start0 % 1000
          val length = length0 % 1000

          // NB: this is the MongoDB behavior, for lack of a better idea
          val expected = StringLib.safeSubstring(str, start, length)
          ternary(Substring(_, _, _).embed, Data.Str(str), Data.Int(start), Data.Int(length), Data.Str(expected))
        }
      }

      "Split" >> {
        "some string" >> {
          binary(Split(_, _).embed, Data.Str("some string"), Data.Str(" "), Data.Arr(List("some", "string").map(Data.Str(_))))
        }
        "some string by itself" >> {
          binary(Split(_, _).embed, Data.Str("some string"), Data.Str("some string"), Data.Arr(List("", "").map(Data.Str(_))))
        }
        "any string not containing delimiter" >> prop { (s: String, d: String) =>
          (!d.isEmpty && !s.contains(d)) ==>
            binary(Split(_, _).embed, Data.Str(s), Data.Str(d), Data.Arr(List(Data.Str(s))))
        }
        "any string with non-empty delimiter" >> prop { (s: String, d: String) =>
          !d.isEmpty ==>
            binary(Split(_, _).embed, Data.Str(s), Data.Str(d), Data.Arr(s.split(Regex.quote(d), -1).toList.map(Data.Str(_))))
        }
      }

      "Boolean" >> {
        "true" >> {
          unary(Boolean(_).embed, Data.Str("true"), Data.Bool(true))
        }

        "false" >> {
          unary(Boolean(_).embed, Data.Str("false"), Data.Bool(false))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Integer" >> {
        "any BigInt in the domain" >> prop { (x: BigInt) =>
          unary(Integer(_).embed, Data.Str(x.toString), Data.Int(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Decimal" >> {
        "any BigDecimal in the domain" >> prop { (x: BigDecimal) =>
          unary(Decimal(_).embed, Data.Str(x.toString), Data.Dec(x))
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "Null" >> {
        "null" >> {
          unary(Null(_).embed, Data.Str("null"), Data.Null)
        }

        // TODO: how to express "should execute and may produce any result"
      }

      "ToString" >> {
        "string" >> prop { (str: String) =>
          unary(ToString(_).embed, Data.Str(str), Data.Str(str))
        }

        "null" >> {
          unary(ToString(_).embed, Data.Null, Data.Str("null"))
        }

        "true" >> {
          unary(ToString(_).embed, Data.Bool(true), Data.Str("true"))
        }

        "false" >> {
          unary(ToString(_).embed, Data.Bool(false), Data.Str("false"))
        }

        "int" >> prop { (x: BigInt) =>
          unary(ToString(_).embed, Data.Int(x), Data.Str(x.toString))
        }

        // TODO: re-parse and compare the resulting value approximately. It's
        // not reasonable to expect a perfect match on formatted values,
        // because of trailing zeros, round-off, and choive of notation.
        // "dec" >> prop { (x: BigDecimal) =>
        //   unary(ToString(_).embed, Data.Dec(x), Data.Str(x.toString))
        // }

        "datetime" >> {
          def test(x: JOffsetDateTime) = unary(
            ToString(_).embed,
            Data.OffsetDateTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(Instant.EPOCH.atOffset(ZoneOffset.UTC))

          "any" >> prop (test(_: JOffsetDateTime))
        }

        "date" >> prop { (x: JLocalDate) =>
          unary(ToString(_).embed, Data.LocalDate(x), Data.Str(x.toString))
        }

        "time" >> {
          def test(x: JLocalTime) = unary(
            ToString(_).embed,
            Data.LocalTime(x),
            Data.Str(x.toString))

          "zero fractional seconds" >> test(JLocalTime.NOON)

          "any" >> prop (test(_: JLocalTime))
        }

        // TODO: Enable
        // "interval" >> prop { (x: Duration) =>
        //   unary(ToString(_).embed, Data.Interval(x), Data.Str(x.toString))
        // }
      }
    }

    "DateLib" >> {
      import DateLib._
      def unaryB[I](
                 prg: Fix[LogicalPlan] => Fix[LogicalPlan],
                 input: I, expected: Data)(implicit arbF: Arbitrary[Builder[I, Data]]): Prop =
        prop { (b: Builder[I, Data]) =>
          unary(prg, b.f(input), expected)
        }

      def unaryBE[I](
                     prg: Fix[LogicalPlan] => Fix[LogicalPlan],
                     input: I, expected: I)(implicit arbF: Arbitrary[Builder[I, Data]]): Prop =
        prop { (b: Builder[I, Data]) =>
          unary(prg, b.f(input), b.f(expected))
        }

      "ExtractCentury" >> {
        "0001-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDate.parse("0001-01-01"), Data.Int(1))
        }

        "2000-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDate.parse("2000-01-01"), Data.Int(20))
        }

        "2001-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDate.parse("2001-01-01"), Data.Int(21))
        }

        "midnight 0001-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDateTime.parse("0001-01-01T00:00:00.000"), Data.Int(1))
        }

        "midnight 2000-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDateTime.parse("2000-01-01T00:00:00.000"), Data.Int(20))
        }

        "midnight 2001-01-01" >> {
          unaryB(ExtractCentury(_).embed, JLocalDateTime.parse("2001-01-01T00:00:00.000"), Data.Int(21))
        }
      }

      "ExtractDayOfMonth" >> {
        "2016-01-01" >> {
          unaryB(ExtractDayOfMonth(_).embed, JLocalDate.parse("2016-01-01"), Data.Int(1))
        }

        "midnight 2016-01-01" >> {
          unaryB(ExtractDayOfMonth(_).embed, JLocalDateTime.parse("2016-01-01T00:00:00.000"), Data.Int(1))
        }

        "2016-02-29" >> {
          unaryB(ExtractDayOfMonth(_).embed, JLocalDate.parse("2016-02-29"), Data.Int(29))
        }

        "midnight 2016-02-29" >> {
          unaryB(ExtractDayOfMonth(_).embed, JLocalDateTime.parse("2016-02-29T00:00:00.000"), Data.Int(29))
        }
      }

      "ExtractDecade" >> {
        "1999-12-31" >> {
          unaryB(ExtractDecade(_).embed, JLocalDate.parse("1999-12-31"), Data.Int(199))
        }

        "midnight 1999-12-31" >> {
          unaryB(ExtractDecade(_).embed, JLocalDateTime.parse("1999-12-31T00:00:00.000"), Data.Int(199))
        }
      }

      "ExtractDayOfWeek" >> {
        "2016-09-28" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDate.parse("2016-09-28"), Data.Int(3))
        }

        "midnight 2016-09-28" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDateTime.parse("2016-09-28T00:00:00.000"), Data.Int(3))
        }

        "2016-10-02" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDate.parse("2016-10-02"), Data.Int(0))
        }

        "midnight 2016-10-02" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDateTime.parse("2016-10-02T00:00:00.000"), Data.Int(0))
        }

        "2016-10-08" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDate.parse("2016-10-08"), Data.Int(6))
        }

        "noon 2016-10-08" >> {
          unaryB(ExtractDayOfWeek(_).embed, JLocalDateTime.parse("2016-10-08T12:00:00.000"), Data.Int(6))
        }
      }

      "ExtractDayOfYear" >> {
        "2016-03-01" >> {
          unaryB(ExtractDayOfYear(_).embed, JLocalDate.parse("2016-03-01"), Data.Int(61))
        }

        "midnight 2016-03-01" >> {
          unaryB(ExtractDayOfYear(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Int(61))
        }

        "2017-03-01" >> {
          unaryB(ExtractDayOfYear(_).embed, JLocalDate.parse("2017-03-01"), Data.Int(60))
        }

        "midnight 2017-03-01" >> {
          unaryB(ExtractDayOfYear(_).embed, JLocalDateTime.parse("2017-03-01T00:00:00.000"), Data.Int(60))
        }
      }

      "ExtractEpoch" >> {
        "2016-09-29 12:34:56.789" >> {
          unary(ExtractEpoch(_).embed, Data.OffsetDateTime(JOffsetDateTime.parse("2016-09-29T12:34:56.789Z")), Data.Dec(1475152496.789))
        }
      }

      "ExtractHour" >> {
        "midnight 2016-09-29" >> {
          unaryB(ExtractHour(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Int(0))
        }

        "2016-09-29 12:34:56.789" >> {
          unaryB(ExtractHour(_).embed, JLocalDateTime.parse("2016-03-01T12:34:56.789"), Data.Int(12))
        }
      }

      "ExtractIsoDayOfWeek" >> {
        "2016-09-28" >> {
          unaryB(ExtractIsoDayOfWeek(_).embed, JLocalDate.parse("2016-09-28"), Data.Int(3))
        }

        "midnight 2016-09-28" >> {
          unaryB(ExtractIsoDayOfWeek(_).embed, JLocalDateTime.parse("2016-09-28T00:00:00.000"), Data.Int(3))
        }

        "2016-10-02" >> {
          unaryB(ExtractIsoDayOfWeek(_).embed, JLocalDate.parse("2016-10-02"), Data.Int(7))
        }

        "midnight 2016-10-02" >> {
          unaryB(ExtractIsoDayOfWeek(_).embed, JLocalDateTime.parse("2016-10-02T00:00:00.000"), Data.Int(7))
        }
      }

      "ExtractIsoYear" >> {
        "2006-01-01" >> {
          unaryB(ExtractIsoYear(_).embed, JLocalDate.parse("2006-01-01"), Data.Int(2005))
        }

        "midnight 2006-01-01" >> {
          unaryB(ExtractIsoYear(_).embed, JLocalDateTime.parse("2006-01-01T00:00:00.000"), Data.Int(2005))
        }

        "2006-01-02" >> {
          unaryB(ExtractIsoYear(_).embed, JLocalDate.parse("2006-01-02"), Data.Int(2006))
        }

        "midnight 2006-01-02" >> {
          unaryB(ExtractIsoYear(_).embed, JLocalDateTime.parse("2006-01-02T00:00:00.000"), Data.Int(2006))
        }
      }

      "ExtractMicrosecond" >> {
        "midnight 2016-09-29" >> {
          unaryB(ExtractMicrosecond(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Dec(0))
        }

        "2016-09-29 12:34:56.789" >> {
          unaryB(ExtractMicrosecond(_).embed, JLocalDateTime.parse("2016-03-01T12:34:56.789"), Data.Dec(56.789e6))
        }
      }

      "ExtractMillennium" >> {
        "0001-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDate.parse("0001-01-01"), Data.Int(1))
        }

        "2000-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDate.parse("2000-01-01"), Data.Int(2))
        }

        "2001-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDate.parse("2001-01-01"), Data.Int(3))
        }

        "midnight 0001-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDateTime.parse("0001-01-01T00:00:00.000"), Data.Int(1))
        }

        "midnight 2000-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDateTime.parse("2000-01-01T00:00:00.000"), Data.Int(2))
        }

        "midnight 2001-01-01" >> {
          unaryB(ExtractMillennium(_).embed, JLocalDateTime.parse("2001-01-01T00:00:00.000"), Data.Int(3))
        }
      }

      "ExtractMillisecond" >> {
        "midnight 2016-09-29" >> {
          unaryB(ExtractMillisecond(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Dec(0))
        }

        "2016-09-29 12:34:56.789" >> {
          unaryB(ExtractMillisecond(_).embed, JLocalDateTime.parse("2016-03-01T12:34:56.789"), Data.Dec(56.789e3))
        }
      }

      "ExtractMinute" >> {
        "midnight 2016-09-29" >> {
          unaryB(ExtractMinute(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Int(0))
        }

        "2016-09-29 12:34:56.789" >> {
          unaryB(ExtractMinute(_).embed, JLocalDateTime.parse("2016-03-01T12:34:56.789"), Data.Int(34))
        }
      }

      "ExtractMonth" >> {
        "2016-01-01" >> {
          unaryB(ExtractMonth(_).embed, JLocalDate.parse("2016-01-01"), Data.Int(1))
        }

        "midnight 2016-01-01" >> {
          unaryB(ExtractMonth(_).embed, JLocalDateTime.parse("2016-01-01T00:00:00.000"), Data.Int(1))
        }

        "2016-02-29" >> {
          unaryB(ExtractMonth(_).embed, JLocalDate.parse("2016-02-29"), Data.Int(2))
        }

        "midnight 2016-02-29" >> {
          unaryB(ExtractMonth(_).embed, JLocalDateTime.parse("2016-02-29T00:00:00.000"), Data.Int(2))
        }
      }

      "ExtractQuarter" >> {
        "2016-10-03" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDate.parse("2016-10-03"), Data.Int(4))
        }

        "midnight 2016-10-03" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDateTime.parse("2016-10-03T00:00:00.000"), Data.Int(4))
        }

        "2016-03-31 (leap year)" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDate.parse("2016-03-31"), Data.Int(1))
        }

        "midnight 2016-03-31 (leap year)" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDateTime.parse("2016-03-31T00:00:00.000"), Data.Int(1))
        }

        "2016-04-01 (leap year)" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDate.parse("2016-04-01"), Data.Int(2))
        }

        "midnight 2016-04-01 (leap year)" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDateTime.parse("2016-04-01T00:00:00.000"), Data.Int(2))
        }

        "2017-03-31" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDate.parse("2017-03-31"), Data.Int(1))
        }

        "midnight 2017-03-31" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDateTime.parse("2017-03-31T00:00:00.000"), Data.Int(1))
        }

        "2017-04-01" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDate.parse("2017-04-01"), Data.Int(2))
        }

        "midnight 2017-04-01" >> {
          unaryB(ExtractQuarter(_).embed, JLocalDateTime.parse("2017-04-01T00:00:00.000"), Data.Int(2))
        }
      }

      "ExtractSecond" >> {
        "midnight 2016-09-29" >> {
          unaryB(ExtractSecond(_).embed, JLocalDateTime.parse("2016-03-01T00:00:00.000"), Data.Dec(0))
        }

        "2016-09-29 12:34:56.789" >> {
          unaryB(ExtractSecond(_).embed, JLocalDateTime.parse("2016-03-01T12:34:56.789"), Data.Dec(56.789))
        }
      }

      "ExtractTimezone" >> {
        "2016-01-01+01:01:01" >> {
          unary(ExtractTimezone(_).embed, Data.OffsetDate(quasar.OffsetDate.parse("2016-01-01+01:01:01")), Data.Int(3661))
        }
        "01:02:03+01:01:01" >> {
          unary(ExtractTimezone(_).embed, Data.OffsetTime(JOffsetTime.parse("01:02:03+01:01:01")), Data.Int(3661))
        }
        "2016-01-01T01:02:03+01:01:01" >> {
          unary(ExtractTimezone(_).embed, Data.OffsetDateTime(JOffsetDateTime.parse("2016-01-01T01:02:03+01:01:01")), Data.Int(3661))
        }
      }

      "ExtractTimezoneMinute" >> {
        "2016-01-01+01:01:01" >> {
          unary(ExtractTimezoneMinute(_).embed, Data.OffsetDate(quasar.OffsetDate.parse("2016-01-01+01:01:01")), Data.Int(1))
        }
        "01:02:03+01:01:01" >> {
          unary(ExtractTimezoneMinute(_).embed, Data.OffsetTime(JOffsetTime.parse("01:02:03+01:01:01")), Data.Int(1))
        }
        "2016-01-01T01:02:03+01:01:01" >> {
          unary(ExtractTimezoneMinute(_).embed, Data.OffsetDateTime(JOffsetDateTime.parse("2016-01-01T01:02:03+01:01:01")), Data.Int(1))
        }
      }

      "ExtractTimezoneHour" >> {
        "2016-01-01+01:01:01" >> {
          unary(ExtractTimezoneHour(_).embed, Data.OffsetDate(quasar.OffsetDate.parse("2016-01-01+01:01:01")), Data.Int(1))
        }
        "01:02:03+01:01:01" >> {
          unary(ExtractTimezoneHour(_).embed, Data.OffsetTime(JOffsetTime.parse("01:02:03+01:01:01")), Data.Int(1))
        }
        "2016-01-01T01:02:03+01:01:01" >> {
          unary(ExtractTimezoneHour(_).embed, Data.OffsetDateTime(JOffsetDateTime.parse("2016-01-01T01:02:03+01:01:01")), Data.Int(1))
        }
      }

      "ExtractWeek" >> {
        "2016-01-01" >> {
          unaryB(ExtractWeek(_).embed, JLocalDate.parse("2016-01-01"), Data.Int(53))
        }

        "midnight 2016-01-01" >> {
          unaryB(ExtractWeek(_).embed, JLocalDateTime.parse("2016-01-01T00:00:00.000"), Data.Int(53))
        }

        "2001-02-16" >> {
          unaryB(ExtractWeek(_).embed, JLocalDate.parse("2001-02-16"), Data.Int(7))
        }

        "midnight 2016-10-03" >> {
          unaryB(ExtractWeek(_).embed, JLocalDateTime.parse("2001-02-16T00:00:00.000"), Data.Int(7))
        }

        "2005-01-01" >> {
          unaryB(ExtractWeek(_).embed, JLocalDate.parse("2005-01-01"), Data.Int(53))
        }

        "midnight 2005-01-01" >> {
          unaryB(ExtractWeek(_).embed, JLocalDateTime.parse("2005-01-01T00:00:00.000"), Data.Int(53))
        }
      }

      "ExtractYear" >> {
        "1999-12-31" >> {
          unaryB(ExtractYear(_).embed, JLocalDate.parse("1999-12-31"), Data.Int(1999))
        }

        "midnight 1999-12-31" >> {
          unaryB(ExtractYear(_).embed, JLocalDateTime.parse("1999-12-31T00:00:00.000"), Data.Int(1999))
        }
      }

      "StartOfDay" >> {
        "datetime" >> prop { (x: JLocalDateTime) =>
          unary(StartOfDay(_).embed, Data.LocalDateTime(x), Data.LocalDateTime(truncDateTime(TemporalPart.Day, x)))
        }

        "date" >> prop { (x: JLocalDate) =>
          unary(
            StartOfDay(_).embed,
            Data.LocalDate(x),
            Data.LocalDateTime(JLocalDateTime.of(x, JLocalTime.MIN)))
        }
      }

      "Now" >> {
        import MathLib.Subtract

        "now" >> prop { (_: Int) =>
          val now = Now[Fix[LogicalPlan]]

          nullary(Subtract(now.embed, now.embed).embed, Data.Interval(DateTimeInterval.zero))
        }
      }

      "OffsetDate" >> prop { (v: JOffsetDate) =>
        unary(OffsetDate(_).embed, Data.Str(v.toString), Data.OffsetDate(v))
      }

      "OffsetDateTime" >> prop { (v: JOffsetDateTime) =>
        unary(OffsetDateTime(_).embed, Data.Str(v.toString), Data.OffsetDateTime(v))
      }

      "OffsetTime" >> prop { (v: JOffsetTime) =>
        unary(OffsetTime(_).embed, Data.Str(v.toString), Data.OffsetTime(v))
      }

      "LocalDate" >> prop { (v: JLocalDate) =>
        unary(LocalDate(_).embed, Data.Str(v.toString), Data.LocalDate(v))
      }

      "LocalDateTime" >> prop { (v: JLocalDateTime) =>
        unary(LocalDateTime(_).embed, Data.Str(v.toString), Data.LocalDateTime(v))
      }

      "LocalTime" >> prop { (v: JLocalTime) =>
        unary(LocalTime(_).embed, Data.Str(v.toString), Data.LocalTime(v))
      }

      "TemporalTrunc" >> {
        def truncOffsetDateTime(p: TemporalPart, i: JOffsetDateTime): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.OffsetDateTime(i),
            Data.OffsetDateTime(JOffsetDateTime.of(truncDateTime(p, i.toLocalDateTime), i.getOffset)))

        "Q#1966" >>
          truncOffsetDateTime(
            TemporalPart.Century,
            JOffsetDateTime.parse("2000-03-01T06:15:45.204Z"))

        "datetime Century" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Century, x)
        }

        "datetime Day" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Day, x)
        }

        "datetime Decade" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Decade, x)
        }

        "datetime Hour" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Hour, x)
        }

        "datetime Microsecond" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Microsecond, x)
        }

        "datetime Millennium" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Millennium, x)
        }

        "datetime Millisecond" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Millisecond, x)
        }

        "datetime Minute" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Minute, x)
        }

        "datetime Month" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Month, x)
        }

        "datetime Quarter" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Quarter, x)
        }

        "datetime Second" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Second, x)
        }

        "datetime Week" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Week, x)
        }

        "datetime Year" >> prop { x: JOffsetDateTime =>
          truncOffsetDateTime(TemporalPart.Year, x)
        }

        def truncDateʹ(p: TemporalPart, d: JLocalDate): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.LocalDate(d),
            Data.LocalDate(truncDate(p, d)))

        "date Century" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Century, d)
        }

        "date Day" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Day, d)
        }

        "date Decade" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Decade, d)
        }

        "date Hour" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Hour, d)
        }

        "date Microsecond" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Microsecond, d)
        }

        "date Millennium" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Millennium, d)
        }

        "date Millisecond" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Millisecond, d)
        }

        "date Minute" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Minute, d)
        }

        "date Month" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Month, d)
        }

        "date Quarter" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Quarter, d)
        }

        "date Second" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Second, d)
        }

        "date Week" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Week, d)
        }

        "date Year" >> prop { d: JLocalDate =>
          truncDateʹ(TemporalPart.Year, d)
        }

        def truncLocalTimeʹ(p: TemporalPart, t: JLocalTime): Result =
          unary(
            TemporalTrunc(p, _).embed,
            Data.LocalTime(t),
            Data.LocalTime(truncTime(p, t)))

        "time Hour" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Hour, t)
        }

        "time Microsecond" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Microsecond, t)
        }

        "time Millisecond" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Millisecond, t)
        }

        "time Minute" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Minute, t)
        }

        "time Second" >> prop { t: JLocalTime =>
          truncLocalTimeʹ(TemporalPart.Second, t)
        }
      }

      "TimeOfDay" >> {
        "datetime" >> {
          val now = JOffsetDateTime.now
          val expected = now.toOffsetTime
          unary(TimeOfDay(_).embed, Data.OffsetDateTime(now), Data.OffsetTime(expected))
        }
      }
    }

    "MathLib" >> {
      import MathLib._

      // NB: testing only (32-bit) ints, to avoid overflowing 64-bit longs
      // and the 53 bits of integer precision in a 64-bit double.

      // TODO: BigDecimals (which can under/overflow)
      // TODO: mixed BigInt/BigDecimal (which can explode)

      "Add" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          binary(Add(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong + y.toLong))
        }

        "any doubles" >> prop { (x: Double, y: Double) =>
          binary(Add(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x + y))
        }

        "mixed int/double" >> prop { (x: Int, y: Double) =>
          commute(Add(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x + y))
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genDateInterval)
          "OffsetDate/Interval" >> prop { (x: JOffsetDate, i: DateTimeInterval) =>
            val dateInterval = (i.seconds == 0) && (i.nanos == 0)
            dateInterval ==> {
              val result = x.date.plusYears(i.years.toLong).plusMonths(i.months.toLong).plusDays(i.days.toLong)
              commute(Add(_, _).embed, Data.OffsetDate(x), Data.Interval(i), Data.OffsetDate(quasar.OffsetDate(result, x.offset)))
            }
          }
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genTimeInterval)
          "OffsetTime/Interval" >> prop { (x: JOffsetTime, i: DateTimeInterval) =>
            val result = x.plusSeconds(i.seconds).plusNanos(i.nanos.toLong)
            commute(Add(_, _).embed, Data.OffsetTime(x), Data.Interval(i), Data.OffsetTime(result))
          }
        }

        "OffsetDateTime/Interval" >> prop { (x: JOffsetDateTime, i: DateTimeInterval) =>
          val result = x
            .plusYears(i.years.toLong).plusMonths(i.months.toLong).plusDays(i.days.toLong)
            .plusSeconds(i.seconds).plusNanos(i.nanos.toLong)
          commute(Add(_, _).embed, Data.OffsetDateTime(x), Data.Interval(i), Data.OffsetDateTime(result))
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genDateInterval)
          "LocalDate/Interval" >> prop { (x: JLocalDate, i: DateTimeInterval) =>
            val result = x.plusYears(i.years.toLong).plusMonths(i.months.toLong).plusDays(i.days.toLong)
            commute(Add(_, _).embed, Data.LocalDate(x), Data.Interval(i), Data.LocalDate(result))
          }
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genTimeInterval)
          "LocalTime/Interval" >> prop { (x: JLocalTime, i: DateTimeInterval) =>
            val result = x.plusSeconds(i.seconds).plusNanos(i.nanos.toLong)
            commute(Add(_, _).embed, Data.LocalTime(x), Data.Interval(i), Data.LocalTime(result))
          }
        }

        "LocalDateTime/Interval" >> prop { (x: JLocalDateTime, i: DateTimeInterval) =>
          val result = x
            .plusYears(i.years.toLong).plusMonths(i.months.toLong).plusDays(i.days.toLong)
            .plusSeconds(i.seconds).plusNanos(i.nanos.toLong)
          commute(Add(_, _).embed, Data.LocalDateTime(x), Data.Interval(i), Data.LocalDateTime(result))
        }
      }

      "Multiply" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          binary(Multiply(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong * y.toLong))
        }

        // TODO: figure out what domain can be tested here (tends to overflow)
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   binary(Multiply(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x * y))
        // }

        // TODO: figure out what domain can be tested here
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   commute(Multiply(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x * y))
        // }

        "int/Interval" >> prop { (x: Int, i: DateTimeInterval) =>
          commute(Multiply(_, _).embed, Data.Int(x), Data.Interval(i), Data.Interval(i.multiply(x)))
        }
      }

      "Power" >> {
        "Int to 0" >> prop { (x: BigInt) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(0), Data.Int(1))
        }

        "Dec to 0" >> prop { (x: BigDecimal) =>
          binary(Power(_, _).embed, Data.Dec(x), Data.Int(0), Data.Int(1))
        }

        "Int to 1" >> prop { (x: BigInt) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(1), Data.Int(x))
        }

        "Dec to 1" >> prop { (x: BigDecimal) =>
          binary(Power(_, _).embed, Data.Dec(x), Data.Int(1), Data.Dec(x))
        }

        "0 to small positive int" >> prop { (y0: Int) =>
          val y = abs(y0 % 10)
          y != 0 ==>
            binary(Power(_, _).embed, Data.Int(0), Data.Int(y), Data.Int(0))
        }

        // TODO: figure out what domain can be tested here (negatives?)
        // "0 to Dec" >> prop { (y: BigDecimal) =>
        //   y != 0 ==>
        //     binary(Power(_, _).embed, Data.Int(0), Data.Dec(y), Data.Int(0))
        // }

        "Int squared" >> prop { (x: Int) =>
          binary(Power(_, _).embed, Data.Int(x), Data.Int(2), Data.Int(x.toLong * x.toLong))
        }

        // TODO: test as much of the domain as much sense
      }

      "Subtract" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          binary(Subtract(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(x.toLong - y.toLong))
        }

        "any doubles" >> prop { (x: Double, y: Double) =>
          binary(Subtract(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x - y))
        }

        "mixed int/double" >> prop { (x: Int, y: Double) =>
          binary(Subtract(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x - y)) and
            binary(Subtract(_, _).embed, Data.Dec(y), Data.Int(x), Data.Dec(y - x))
        }

        "LocalDateTime/Interval" >> prop { (x: JLocalDateTime, y: DateTimeInterval) =>
           binary(Subtract(_, _).embed, Data.LocalDateTime(x), Data.Interval(y), Data.LocalDateTime(y.subtractFrom(x)))
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genDateInterval)
          "LocalDate/Interval" >> prop { (x: JLocalDate, y: DateTimeInterval) =>
            binary(Subtract(_, _).embed, Data.LocalDate(x), Data.Interval(y), Data.LocalDate(x.minus(y.toPeriod)))
          }
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genTimeInterval)
          "LocalTime/Interval" >> prop { (x: JLocalTime, y: DateTimeInterval) =>
            binary(Subtract(_, _).embed, Data.LocalTime(x), Data.Interval(y), Data.LocalTime(x.minus(y.toDuration)))
          }
        }

        "OffsetDateTime/Interval" >> prop { (x: JOffsetDateTime, y: DateTimeInterval) =>
          binary(Subtract(_, _).embed, Data.OffsetDateTime(x), Data.Interval(y), Data.OffsetDateTime(y.subtractFromOffset(x)))
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genDateInterval)
          "OffsetDate/Interval" >> prop { (x: quasar.OffsetDate, y: DateTimeInterval) =>
            binary(Subtract(_, _).embed, Data.OffsetDate(x), Data.Interval(y), Data.OffsetDate(x.minus(y.toPeriod)))
          }
        }

        {
          implicit val arbInterval = Arbitrary(DateGenerators.genTimeInterval)
          "OffsetTime/Interval" >> prop { (x: JOffsetTime, y: DateTimeInterval) =>
            binary(Subtract(_, _).embed, Data.OffsetTime(x), Data.Interval(y), Data.OffsetTime(x.minus(y.toDuration)))
          }
        }

        // TODO: LocalDateTime/LocalDateTime, LocalDate, LocalTime/LocalTime
      }

      "Divide" >> {
        "any ints" >> prop { (x: Int, y: Int) =>
          y != 0 ==>
            binary(Divide(_, _).embed, Data.Int(x), Data.Int(y), Data.Dec(x.toDouble / y.toDouble))
        }

        // TODO: figure out what domain can be tested here
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   binary(Divide(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x / y))
        // }

        // TODO: figure out what domain can be tested here
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   commute(Divide(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(x / y))
        // }

        // TODO: Interval * Int
      }

      "Negate" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Negate(_).embed, Data.Int(x), Data.Int(-x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Negate(_).embed, Data.Dec(x), Data.Dec(-x))
        }

        "any Interval" >> prop { (x: DateTimeInterval) =>
          unary(Negate(_).embed, Data.Interval(x), Data.Interval(x.multiply(-1)))
        }
      }

      "Abs" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Abs(_).embed, Data.Int(x), Data.Int(x.abs))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Abs(_).embed, Data.Dec(x), Data.Dec(x.abs))
        }
      }

      "Trunc" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Trunc(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Trunc(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.DOWN)))
        }
      }

      "Ceil" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Ceil(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Ceil(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.CEILING)))
        }
      }

      "Floor" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Floor(_).embed, Data.Int(x), Data.Int(x))
        }

        "any Dec" >> prop { (x: BigDecimal) =>
          unary(Floor(_).embed, Data.Dec(x), Data.Dec(x.setScale(0, RoundingMode.FLOOR)))
        }
      }

      "Round" >> {
        "any Int" >> prop { (x: BigInt) =>
          unary(Round(_).embed, Data.Int(x), Data.Int(x))
        }

        "0.5 -> 0" >> {
          unary(Round(_).embed, Data.Dec(0.5), Data.Int(0))
        }

        "1.5 -> 2" >> {
          unary(Round(_).embed, Data.Dec(1.5), Data.Int(2))
        }

        "1.75 -> 2" >> {
          unary(Round(_).embed, Data.Dec(1.75), Data.Int(2))
        }

        "2.5 -> 2" >> {
          unary(Round(_).embed, Data.Dec(2.5), Data.Int(2))
        }

        "2.75 -> 3" >> {
          unary(Round(_).embed, Data.Dec(2.75), Data.Int(3))
        }

        "-0.5 -> 0" >> {
          unary(Round(_).embed, Data.Dec(-0.5), Data.Int(0))
        }

        "-1.5 -> -2" >> {
          unary(Round(_).embed, Data.Dec(-1.5), Data.Int(-2))
        }

        "-2.5 -> -2" >> {
          unary(Round(_).embed, Data.Dec(-2.5), Data.Int(-2))
        }
      }

      "CeilScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 1" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(1))
          }

          "1.5 -> 2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(2))
          }

          "2.5 -> 3" >> {
            binary(CeilScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(3))
          }

          "-0.5 -> 0" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(0))
          }

          "-1.5 -> -1" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-1))
          }

          "-2.5 -> -2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-2))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.3" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.3))
          }

          "-0.5 -> -0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.2" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.2))
          }
        }

        "scale 2" >> {
          val scale = 2

          "any Int" >> prop { (x: BigInt) =>
            binary(CeilScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.25" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.25))
          }

          "0.125 -> 0.13" >> {
            binary(CeilScale(_, _).embed, Data.Dec(0.125), Data.Int(scale), Data.Dec(0.13))
          }

          "-0.5 -> -0.5" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.25" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.25))
          }

          "-0.125 -> -0.12" >> {
            binary(CeilScale(_, _).embed, Data.Dec(-0.125), Data.Int(scale), Data.Dec(-0.12))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 10" >> {
            binary(CeilScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(10))
          }

          "10 -> 10" >> {
            binary(CeilScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12350" >> {
            binary(CeilScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12350))
          }
        }
      }

      "FloorScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(0))
          }

          "1.5 -> 1" >> {
            binary(FloorScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(1))
          }

          "2.5 -> 2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(2))
          }

          "-0.5 -> -1" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(-1))
          }

          "-1.5 -> -2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-2))
          }

          "-2.5 -> -3" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-3))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.2" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.2))
          }

          "-0.5 -> -0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> -0.3" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.3))
          }
        }

        "scale 2" >> {
          val scale = 2

          "any Int" >> prop { (x: BigInt) =>
            binary(FloorScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "0.25 -> 0.25" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.25), Data.Int(scale), Data.Dec(0.25))
          }

          "0.125 -> 0.12" >> {
            binary(FloorScale(_, _).embed, Data.Dec(0.125), Data.Int(scale), Data.Dec(0.12))
          }

          "-0.5 -> -0.5" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-0.25 -> 0.25" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.25), Data.Int(scale), Data.Dec(-0.25))
          }

          "-0.125 -> -0.13" >> {
            binary(FloorScale(_, _).embed, Data.Dec(-0.125), Data.Int(scale), Data.Dec(-0.13))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 0" >> {
            binary(FloorScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(0))
          }

          "10 -> 10" >> {
            binary(FloorScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12340" >> {
            binary(FloorScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12340))
          }
        }
      }

      "RoundScale" >> {
        "scale 0" >> {
          val scale = 0

          "any Int" >> prop { (x: BigInt) =>
            binary(RoundScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Int(0))
          }

          "1.5 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Int(2))
          }

          "1.75 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.75), Data.Int(scale), Data.Int(2))
          }

          "2.5 -> 2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(2.5), Data.Int(scale), Data.Int(2))
          }

          "2.75 -> 3" >> {
            binary(RoundScale(_, _).embed, Data.Dec(2.75), Data.Int(scale), Data.Int(3))
          }

          "-0.5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Int(0))
          }

          "-1.5 -> -2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Int(-2))
          }

          "-2.5 -> -2" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-2.5), Data.Int(scale), Data.Int(-2))
          }
        }

        "scale 1" >> {
          val scale = 1

          "any Int" >> prop { (x: BigInt) =>
            binary(RoundScale(_, _).embed, Data.Int(x), Data.Int(scale), Data.Int(x))
          }

          "0.5 -> 0.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(0.5), Data.Int(scale), Data.Dec(0.5))
          }

          "1.5 -> 1.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.5), Data.Int(scale), Data.Dec(1.5))
          }

          "1.75 -> 1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.75), Data.Int(scale), Data.Dec(1.8))
          }

          "1.65 -> 1.6" >> {
            binary(RoundScale(_, _).embed, Data.Dec(1.65), Data.Int(scale), Data.Dec(1.6))
          }

          "-0.5 -> -0.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-0.5), Data.Int(scale), Data.Dec(-0.5))
          }

          "-1.5 -> -1.5" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.5), Data.Int(scale), Data.Dec(-1.5))
          }

          "-1.75 -> -1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.75), Data.Int(scale), Data.Dec(-1.8))
          }

          "-1.85 -> -1.8" >> {
            binary(RoundScale(_, _).embed, Data.Dec(-1.85), Data.Int(scale), Data.Dec(-1.8))
          }
        }

        "scale -1" >> {
          val scale = -1

          "1 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Int(1), Data.Int(scale), Data.Int(0))
          }

          "5 -> 0" >> {
            binary(RoundScale(_, _).embed, Data.Int(5), Data.Int(scale), Data.Int(0))
          }

          "10 -> 10" >> {
            binary(RoundScale(_, _).embed, Data.Int(10), Data.Int(scale), Data.Int(10))
          }

          "12345 -> 12340" >> {
            binary(RoundScale(_, _).embed, Data.Int(12345), Data.Int(scale), Data.Int(12340))
          }

          "12335 -> 12340" >> {
            binary(RoundScale(_, _).embed, Data.Int(12335), Data.Int(scale), Data.Int(12340))
          }

          "123 -> 120" >> {
            binary(RoundScale(_, _).embed, Data.Int(123), Data.Int(scale), Data.Int(120))
          }
        }
      }

      "Modulo" >> {
        "any int by 1" >> prop { (x: Int) =>
          binary(Modulo(_, _).embed, Data.Int(x), Data.Int(1), Data.Int(0))
        }

        "any ints" >> prop { (x: Int, y: Int) =>
          y != 0 ==>
            binary(Modulo(_, _).embed, Data.Int(x), Data.Int(y), Data.Int(BigInt(x) % BigInt(y)))
        }

        // TODO analyze and optionally shortCircuit per connector
        // "any doubles" >> prop { (x: Double, y: Double) =>
        //   y != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(BigDecimal(x).remainder(BigDecimal(y))))
        // }
        //
        // "any big decimals" >> prop { (x: BigDecimal, y: BigDecimal) =>
        //   !y.equals(0.0) ==>
        //     binary(Modulo(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Dec(x.remainder(y)))
        // }
        //
        // "mixed int/double" >> prop { (x: Int, y: Double) =>
        //   y != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Int(x), Data.Dec(y), Data.Dec(BigDecimal(y).remainder(BigDecimal(x))))
        //   x != 0 ==>
        //     binary(Modulo(_, _).embed, Data.Dec(y), Data.Int(x), Data.Dec(BigDecimal(y).remainder(BigDecimal(x))))
        // }
      }
    }

    "RelationsLib" >> {
      import RelationsLib._

      "Eq" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Eq(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x == y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Eq(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x == y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Eq(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x == y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Eq(_, _).embed, x, x, Data.Bool(true))
        }

        "any values with different types" >> prop { (x: Data, y: Data) =>
          // ...provided they are not both Numeric (Int | Dec)
          (x.dataType != y.dataType &&
            !((Type.Numeric contains x.dataType) &&
              (Type.Numeric contains y.dataType))) ==>
            binary(Eq(_, _).embed, x, y, Data.Bool(false))
        }

      }

      "Neq" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Neq(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x != y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Neq(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x != y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Neq(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x != y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Neq(_, _).embed, x, x, Data.Bool(false))
        }

        "any values with different types" >> prop { (x: Data, y: Data) =>
          // ...provided they are not both Numeric (Int | Dec)
          (x.dataType != y.dataType &&
            !((Type.Numeric contains x.dataType) &&
              (Type.Numeric contains y.dataType))) ==>
            binary(Neq(_, _).embed, x, y, Data.Bool(true))
        }

        // TODO: the rest of the types
      }

      "Lt" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Lt(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x < y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Lt(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x < y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Lt(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x < y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Lt(_, _).embed, x, x, Data.Bool(false))
        }
      }

      "Lte" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Lte(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x <= y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Lte(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x <= y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Lte(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x <= y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Lte(_, _).embed, x, x, Data.Bool(true))
        }
      }

      "Gt" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Gt(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x > y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Gt(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x > y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Gt(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x > y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Gt(_, _).embed, x, x, Data.Bool(false))
        }
      }

      "Gte" >> {
        "any two Ints" >> prop { (x: BigInt, y: BigInt) =>
          binary(Gte(_, _).embed, Data.Int(x), Data.Int(y), Data.Bool(x >= y))
        }

        "any two Decs" >> prop { (x: BigDecimal, y: BigDecimal) =>
          binary(Gte(_, _).embed, Data.Dec(x), Data.Dec(y), Data.Bool(x >= y))
        }

        "any two Strs" >> prop { (x: String, y: String) =>
          binary(Gte(_, _).embed, Data.Str(x), Data.Str(y), Data.Bool(x >= y))
        }

        "any value with self" >> prop { (x: Data) =>
          binary(Gte(_, _).embed, x, x, Data.Bool(true))
        }

      }

      "Between" >> {
        "any three Ints" >> prop { (lo: BigInt, mid: BigInt, hi: BigInt) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Int(mid), Data.Int(lo), Data.Int(hi), Data.Bool(result))
        }

        "any three Decs" >> prop { (lo: BigDecimal, mid: BigDecimal, hi: BigDecimal) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Dec(mid), Data.Dec(lo), Data.Dec(hi), Data.Bool(result))
        }

        "any three Strs" >> prop { (lo: String, mid: String, hi: String) =>
          val result = lo <= mid && mid <= hi
          ternary(Between(_, _, _).embed, Data.Str(mid), Data.Str(lo), Data.Str(hi), Data.Bool(result))
        }

        "any three Bools" >> prop { (lo: Boolean, mid: Boolean, hi: Boolean) =>
          val result = (lo, mid, hi) match {
            case (false, true, true)   => true
            case (false, false, true)  => true
            case (true, true, true)    => true
            case (false, false, false) => true
            case _ => false
          }
          ternary(Between(_, _, _).embed, Data.Bool(mid), Data.Bool(lo), Data.Bool(hi), Data.Bool(result))
        }

        "any three OffsetDateTimes" >> prop { (lo: JOffsetDateTime, mid: JOffsetDateTime, hi: JOffsetDateTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetDateTime(mid), Data.OffsetDateTime(lo), Data.OffsetDateTime(hi), Data.Bool(result))
        }

        "any three OffsetDates" >> prop { (lo: JOffsetDate, mid: JOffsetDate, hi: JOffsetDate) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetDate(mid), Data.OffsetDate(lo), Data.OffsetDate(hi), Data.Bool(result))
        }

        "any three OffsetTimes" >> prop { (lo: JOffsetTime, mid: JOffsetTime, hi: JOffsetTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.OffsetTime(mid), Data.OffsetTime(lo), Data.OffsetTime(hi), Data.Bool(result))
        }

        "any three LocalDateTimes" >> prop { (lo: JLocalDateTime, mid: JLocalDateTime, hi: JLocalDateTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalDateTime(mid), Data.LocalDateTime(lo), Data.LocalDateTime(hi), Data.Bool(result))
        }

        "any three LocalDates" >> prop { (lo: JLocalDate, mid: JLocalDate, hi: JLocalDate) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalDate(mid), Data.LocalDate(lo), Data.LocalDate(hi), Data.Bool(result))
        }

        "any three LocalTimes" >> prop { (lo: JLocalTime, mid: JLocalTime, hi: JLocalTime) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.LocalTime(mid), Data.LocalTime(lo), Data.LocalTime(hi), Data.Bool(result))
        }

        "any three Intervals" >> prop { (lo: DateTimeInterval, mid: DateTimeInterval, hi: DateTimeInterval) =>
          val result = lo.compareTo(mid) <= 0 && mid.compareTo(hi) <= 0
          ternary(Between(_, _, _).embed, Data.Interval(mid), Data.Interval(lo), Data.Interval(hi), Data.Bool(result))
        }

        // TODO: Cross-type comparison
      }

      "IfUndefined" >> {
        """NA ?? 42""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.NA,
            Data.Int(42),
            Data.Int(42))
        }

        """1 ?? 2""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Int(1),
            Data.Int(2),
            Data.Int(1))
        }

        """{"a": 1} ?? 2""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Obj("a" -> Data.Int(1)),
            Data.Int(2),
            Data.Obj("a" -> Data.Int(1)))
        }

        """{"a": NA, "b": 2} ?? 3""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Obj("a" -> Data.NA, "b" -> Data.Int(2)),
            Data.Int(3),
            Data.Obj("b" -> Data.Int(2)))
        }

        """[NA, 2] ?? 3""" >> {
          binary(
            IfUndefined(_, _).embed,
            Data.Arr(Data.NA :: Data.Int(2) :: Nil),
            Data.Int(3),
            Data.Arr(Data.Int(2) :: Nil))
        }
      }

      "And" >> {
        "false, false" >> {
          binary(And(_, _).embed, Data.Bool(false), Data.Bool(false), Data.Bool(false))
        }

        "false, true" >> {
          commute(And(_, _).embed, Data.Bool(false), Data.Bool(true), Data.Bool(false))
        }

        "true, true" >> {
          binary(And(_, _).embed, Data.Bool(true), Data.Bool(true), Data.Bool(true))
        }
      }

      "Or" >> {
        "false, false" >> {
          binary(Or(_, _).embed, Data.Bool(false), Data.Bool(false), Data.Bool(false))
        }

        "false, true" >> {
          commute(Or(_, _).embed, Data.Bool(false), Data.Bool(true), Data.Bool(true))
        }

        "true, true" >> {
          binary(Or(_, _).embed, Data.Bool(true), Data.Bool(true), Data.Bool(true))
        }
      }

      "Not" >> {
        "false" >> {
          unary(Not(_).embed, Data.Bool(false), Data.Bool(true))
        }

        "true" >> {
          unary(Not(_).embed, Data.Bool(true), Data.Bool(false))
        }
      }

      "Cond" >> {
        "true" >> prop { (x: Data, y: Data) =>
          ternary(Cond(_, _, _).embed, Data.Bool(true), x, y, x)
        }

        "false" >> prop { (x: Data, y: Data) =>
          ternary(Cond(_, _, _).embed, Data.Bool(false), x, y, y)
        }
      }
    }

    "StructuralLib" >> {
      import StructuralLib._

      // FIXME: No idea why this is necessary, but ScalaCheck arbContainer
      //        demands it and can't seem to find one in this context.
      implicit def listToTraversable[A](as: List[A]): Traversable[A] = as

      "ConcatOp" >> {
        "array  || array" >> prop { (xs: List[BigInt], ys: List[BigInt]) =>
          val (xints, yints) = (xs map (Data._int(_)), ys map (Data._int(_)))
          binary(ConcatOp(_, _).embed, Data._arr(xints), Data._arr(yints), Data._arr(xints ::: yints))
        }

        "array  || string" >> prop { (xs: List[BigInt], y: String) =>
          val (xints, ystrs) = (xs map (Data._int(_)), y.toList map (c => Data._str(c.toString)))
          binary(ConcatOp(_, _).embed, Data._arr(xints), Data._str(y), Data._arr(xints ::: ystrs))
        }

        "string || array" >> prop { (x: String, ys: List[BigInt]) =>
          val (xstrs, yints) = (x.toList map (c => Data._str(c.toString)), ys map (Data._int(_)))
          binary(ConcatOp(_, _).embed, Data._str(x), Data._arr(yints), Data._arr(xstrs ::: yints))
        }

        "string || string" >> prop { (x: String, y: String) =>
          binary(ConcatOp(_, _).embed, Data._str(x), Data._str(y), Data._str(x + y))
        }
      }

      "MapProject" >> {
        """({"a":1}).a""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1)),
            Data.Str("a"),
            Data.Int(1))
        }

        """({"a":1, "b":2}).b""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("b"),
            Data.Int(2))
        }

        """({"a":1, "b":2}).c""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("c"),
            Data.NA)
        }

        """({}).c""" >> {
          binary(
            MapProject(_, _).embed,
            Data.Obj(),
            Data.Str("c"),
            Data.NA)
        }
      }

      "DeleteKey" >> {
        "{a:1, b:2} delete .a" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("a"),
            Data.Obj("b" -> Data.Int(2)))
        }

        "{a:1, b:2} delete .b" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("b"),
            Data.Obj("a" -> Data.Int(1)))
        }

        "{a:1, b:2} delete .c" >> {
          binary(
            DeleteKey(_, _).embed,
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)),
            Data.Str("c"),
            Data.Obj("a" -> Data.Int(1), "b" -> Data.Int(2)))
        }
      }

      "Meta" >> {
        // FIXME: Implement once we've switched to EJson in LogicalPlan.
        "returns metadata associated with a value" >> pending("Requires EJson.")
      }
    }

    "SetLib" >> {
      import SetLib._

      "Within" >> {
        "0 in [1, 2, 3]" >> {
          binary(Within(_, _).embed, Data.Int(0), Data.Arr(List(Data.Int(1), Data.Int(2), Data.Int(3))), Data.False)
        }

        "1 in [1, 2, 3]" >> {
          binary(Within(_, _).embed, Data.Int(1), Data.Arr(List(Data.Int(1), Data.Int(2), Data.Int(3))), Data.True)
        }

        "0 in []" >> {
          binary(Within(_, _).embed, Data.Int(0), Data.Arr(Nil), Data.False)
        }

        "[0] in [[1], 2, {a:3}, [0]]" >> {
          binary(
            Within(_, _).embed,
            Data.Arr(List(Data.Int(0))),
            Data.Arr(
              List(
                Data.Arr(List(Data.Int(1))),
                Data.Int(2),
                Data.Obj(ListMap("a" -> Data.Int(3))),
                Data.Arr(List(Data.Int(0))))),
            Data.True)
        }

        "[0, 1] in [[1], 2, {a:3}, [0, 1]]" >> {
          binary(
            Within(_, _).embed,
            Data.Arr(List(Data.Int(0), Data.Int(1))),
            Data.Arr(
              List(
                Data.Arr(List(Data.Int(1))),
                Data.Int(2),
                Data.Obj(ListMap("a" -> Data.Int(3))),
                Data.Arr(List(Data.Int(0), Data.Int(1))))),
            Data.True)
        }
      }
    }
  }
}
