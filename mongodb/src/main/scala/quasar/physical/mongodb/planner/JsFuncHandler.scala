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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.javascript.Js
import quasar.jscore, jscore.{Name, JsCoreF}
import quasar.std.StdLib._
import quasar.qscript.MapFuncCore
import quasar.qscript.MapFuncsCore, MapFuncsCore._
import quasar.std.TemporalPart._

import scalaz.{Divide => _, _}, Scalaz._

object JsFuncHandler {
  def apply[T[_[_]], MF[_], A]
      (func: MF[A])
      (implicit MFC: MapFuncCore[T, ?] :<: MF)
      : Option[Free[JsCoreF, A]] = {
    type JS = Free[JsCoreF, A]

    implicit def hole(a: A): JS = Free.pure(a)

    val mjs = quasar.physical.mongodb.javascript[JS](Free.roll)
    import mjs._
    import mjs.js._

    // NB: Math.trunc is not present in MongoDB.
    def trunc(expr: JS): JS =
      Let(Name("x"), expr,
        BinOp(jscore.Sub,
          ident("x"),
          BinOp(jscore.Mod, ident("x"), litNum(1))))

    def dateZ(year: JS, month: JS, day: JS, hr: JS, min: JS, sec: JS, ms: JS): JS =
      New(Name("Date"), List(
        Call(select(ident("Date"), "parse"), List(
          binop(jscore.Add,
            pad4(year), litStr("-"), pad2(month), litStr("-"), pad2(day), litStr("T"),
            pad2(hr), litStr(":"), pad2(min), litStr(":"), pad2(sec), litStr("."),
            pad3(ms), litStr("Z"))))))

    def year(date: JS): JS =
      Call(select(date, "getUTCFullYear"), Nil)

    def month(date: JS): JS =
      binop(jscore.Add, Call(select(date, "getUTCMonth"), Nil), litNum(1))

    def day(date: JS): JS =
      Call(select(date, "getUTCDate"), Nil)

    def hour(date: JS): JS =
      Call(select(date, "getUTCHours"), Nil)

    def minute(date: JS): JS =
      Call(select(date, "getUTCMinutes"), Nil)

    def second(date: JS): JS =
      Call(select(date, "getUTCSeconds"), Nil)

    def millisecond(date: JS): JS =
      Call(select(date, "getUTCMilliseconds"), Nil)

    def dayOfWeek(date: JS): JS =
      Call(select(date, "getUTCDay"), Nil)

    def quarter(date: JS): JS =
      BinOp(jscore.Add,
        Call(select(ident("Math"), "floor"), List(
          BinOp(jscore.Div, Call(select(date, "getUTCMonth"), Nil), litNum(3)))),
        litNum(1))

    def decade(date: JS): JS =
      trunc(BinOp(jscore.Div, year(date), litNum(10)))

    def century(date: JS): JS =
      Call(select(ident("Math"), "ceil"), List(
        BinOp(jscore.Div, year(date), litNum(100))))

    def millennium(date: JS): JS =
      Call(select(ident("Math"), "ceil"), List(
        BinOp(jscore.Div, year(date), litNum(1000))))

    def litStr(s: String): JS =
      Literal(Js.Str(s))

    def litNum(i: Int): JS =
      Literal(Js.Num(i.toDouble, false))

    def pad2(x: JS) =
      Let(Name("x"), x,
        If(
          BinOp(jscore.Lt, ident("x"), litNum(10)),
          BinOp(jscore.Add, litStr("0"), ident("x")),
          ident("x")))

    def pad3(x: JS) =
      Let(Name("x"), x,
        If(
          BinOp(jscore.Lt, ident("x"), litNum(10)),
          BinOp(jscore.Add, litStr("00"), ident("x")),
          If(
            BinOp(jscore.Lt, ident("x"), litNum(100)),
            BinOp(jscore.Add, litStr("0"), ident("x")),
            ident("x"))))

    def pad4(x: JS) =
      Let(Name("x"), x,
        If(
          BinOp(jscore.Lt, ident("x"), litNum(10)),
          BinOp(jscore.Add, litStr("000"), ident("x")),
          If(
            BinOp(jscore.Lt, ident("x"), litNum(100)),
            BinOp(jscore.Add, litStr("00"), ident("x")),
            If(
              BinOp(jscore.Lt, ident("x"), litNum(1000)),
              BinOp(jscore.Add, litStr("0"), ident("x")),
              ident("x")))))

    func.some collect {
      case MFC(Add(a1, a2))      => BinOp(jscore.Add, a1, a2)
      case MFC(Multiply(a1, a2)) => BinOp(jscore.Mult, a1, a2)
      case MFC(Power(a1, a2)) =>
        Call(select(ident("Math"), "pow"), List(a1, a2))
      case MFC(Subtract(a1, a2)) => BinOp(jscore.Sub, a1, a2)
      case MFC(Divide(a1, a2))   => BinOp(jscore.Div, a1, a2)
      case MFC(Modulo(a1, a2))   => BinOp(jscore.Mod, a1, a2)
      case MFC(Negate(a1))       => UnOp(jscore.Neg, a1)

      case MFC(MapFuncsCore.Eq(a1, a2))
                                 => BinOp(jscore.Eq, a1, a2)
      case MFC(Neq(a1, a2)) => BinOp(jscore.Neq, a1, a2)
      case MFC(Lt(a1, a2))  => BinOp(jscore.Lt, a1, a2)
      case MFC(Lte(a1, a2)) => BinOp(jscore.Lte, a1, a2)
      case MFC(Gt(a1, a2))  => BinOp(jscore.Gt, a1, a2)
      case MFC(Gte(a1, a2)) => BinOp(jscore.Gte, a1, a2)
      case MFC(Not(a1))     => UnOp(jscore.Not, a1)
      case MFC(And(a1, a2)) => BinOp(jscore.And, a1, a2)
      case MFC(Or(a1, a2))  => BinOp(jscore.Or, a1, a2)
      case MFC(Between(value, min, max)) =>
          BinOp(jscore.And,
            BinOp(jscore.Lte, min, value),
            BinOp(jscore.Lte, value, max))

      case MFC(MakeArray(a1)) => Arr(List(a1))
      case MFC(Length(str)) =>
        Call(ident("NumberLong"), List(select(hole(str), "length")))
      case MFC(Substring(field, start, len)) =>
        If(BinOp(jscore.Lt, start, litNum(0)),
          litStr(""),
          If(BinOp(jscore.Lt, len, litNum(0)),
            Call(select(field, "substr"), List(start, select(field, "length"))),
            Call(select(field, "substr"), List(start, len))))
      case MFC(Search(field, pattern, insen)) =>
          Call(
            select(
              New(Name("RegExp"), List(
                pattern,
                If(insen, litStr("im"), litStr("m")))),
              "test"),
            List(field))
      case MFC(Null(str)) =>
        If(
          BinOp(jscore.Eq, str, litStr("null")),
          Literal(Js.Null),
          ident("undefined"))
      case MFC(Bool(str)) =>
        If(
          BinOp(jscore.Eq, str, litStr("true")),
          Literal(Js.Bool(true)),
          If(
            BinOp(jscore.Eq, str, litStr("false")),
            Literal(Js.Bool(false)),
            ident("undefined")))
      case MFC(Integer(str)) =>
        If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.intRegex + "$"))), "test"), List(str)),
          Call(ident("NumberLong"), List(str)),
          ident("undefined"))
      case MFC(Decimal(str)) =>
          If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.floatRegex + "$"))), "test"), List(str)),
            Call(ident("parseFloat"), List(str)),
            ident("undefined"))
      case MFC(Date(str)) =>
        If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.dateRegex + "$"))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined"))
      case MFC(Time(str)) =>
        If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.timeRegex + "$"))), "test"), List(str)),
          str,
          ident("undefined"))
      case MFC(Timestamp(str)) =>
        If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.timestampRegex + "$"))), "test"), List(str)),
          Call(ident("ISODate"), List(str)),
          ident("undefined"))
      // TODO: case Interval(str) =>
      case MFC(ToString(value)) =>
        If(isInt(value),
          // NB: This is a terrible way to turn an int into a string, but the
          //     only one that doesn’t involve converting to a decimal and
          //     losing precision.
          Call(select(Call(ident("String"), List(value)), "replace"), List(
            Call(ident("RegExp"), List(
              litStr("[^-0-9]+"),
              litStr("g"))),
            litStr(""))),
          If(BinOp(jscore.Or, isTimestamp(value), isDate(value)),
            Call(select(value, "toISOString"), Nil),
            Call(ident("String"), List(value))))
      // TODO: case ToTimestamp(str) =>

      case MFC(TimeOfDay(date)) =>
        Let(Name("t"), date,
          binop(jscore.Add,
            pad2(hour(ident("t"))),
            litStr(":"),
            pad2(minute(ident("t"))),
            litStr(":"),
            pad2(second(ident("t"))),
            litStr("."),
            pad3(millisecond(ident("t")))))

      case MFC(ExtractCentury(date)) => century(date)
      case MFC(ExtractDayOfMonth(date)) => day(date)
      case MFC(ExtractDecade(date)) => decade(date)
      case MFC(ExtractDayOfWeek(date)) => dayOfWeek(date)
      // TODO: case ExtractDayOfYear(date) =>
      case MFC(ExtractEpoch(date)) =>
        BinOp(jscore.Div,
          Call(select(date, "valueOf"), Nil),
          litNum(1000))
      case MFC(ExtractHour(date)) => hour(date)
      case MFC(ExtractIsoDayOfWeek(date)) =>
        Let(Name("x"), dayOfWeek(date),
          If(
            BinOp(jscore.Eq, ident("x"), litNum(0)),
            litNum(7),
            ident("x")))
      // TODO: case ExtractIsoYear(date) =>
      case MFC(ExtractMicroseconds(date)) =>
        BinOp(jscore.Mult,
          BinOp(jscore.Add,
            millisecond(date),
            BinOp(jscore.Mult,
              second(date),
              litNum(1000))),
          litNum(1000))
      case MFC(ExtractMillennium(date)) => millennium(date)
      case MFC(ExtractMilliseconds(date)) =>
        BinOp(jscore.Add,
          millisecond(date),
          BinOp(jscore.Mult,
            second(date),
            litNum(1000)))
      case MFC(ExtractMinute(date)) => minute(date)
      case MFC(ExtractMonth(date)) => month(date)
      case MFC(ExtractQuarter(date)) => quarter(date)
      case MFC(ExtractSecond(date)) =>
        BinOp(jscore.Add,
          second(date),
          BinOp(jscore.Div, millisecond(date), litNum(1000)))
      // TODO: case MFC(ExtractWeek(date)) =>
      case MFC(ExtractYear(date)) => year(date)

      case MFC(StartOfDay(date)) =>
        dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0))

      case MFC(TemporalTrunc(Century, date)) =>
        val yr =
          Call(select(ident("Math"), "floor"), List(BinOp(jscore.Div, year(date), litNum(100))))
        dateZ(
          BinOp(jscore.Mult, yr, litNum(100)),
          litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Day, date)) =>
        dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Decade, date)) =>
        dateZ(
          BinOp(jscore.Mult, decade(date), litNum(10)),
          litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Hour, date)) =>
        dateZ(year(date), month(date), day(date), hour(date), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Millennium, date)) =>
        dateZ(
          BinOp(jscore.Mult,
            Call(select(ident("Math"), "floor"), List(
              BinOp(jscore.Div, year(date), litNum(1000)))),
            litNum(1000)),
          litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Microsecond | Millisecond, date)) =>
        dateZ(
          year(date), month(date), day(date),
          hour(date), minute(date), second(date), millisecond(date))
      case MFC(TemporalTrunc(Minute, date)) =>
        dateZ(year(date), month(date), day(date), hour(date), minute(date), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Month, date)) =>
        dateZ(year(date), month(date), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Quarter, date)) =>
        dateZ(
          year(date),
          BinOp(jscore.Add,
            BinOp(jscore.Mult,
              BinOp(jscore.Sub, quarter(date), litNum(1)),
              litNum(3)),
            litNum(1)),
          litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
      case MFC(TemporalTrunc(Second, date)) =>
        dateZ(year(date), month(date), day(date), hour(date), minute(date), second(date), litNum(0))
      case MFC(TemporalTrunc(Week, date)) =>
        val d =
          New(Name("Date"), List(
            BinOp(jscore.Sub,
              Call(select(date, "getTime"), Nil),
              BinOp(jscore.Mult,
                litNum(24*60*60*1000),
                BinOp(jscore.Mod,
                  BinOp(jscore.Add, dayOfWeek(date), litNum(6)),
                  litNum(7))
            ))))
        Let(Name("d"), d,
          dateZ(year(d), month(d), day(d), litNum(0), litNum(0), litNum(0), litNum(0)))
      case MFC(TemporalTrunc(Year, date)) =>
        dateZ(year(date), litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))

      case MFC(Now()) => Call(ident("ISODate"), Nil)

      case MFC(ProjectField(obj, field)) => Access(obj, field)
      case MFC(ProjectIndex(arr, index)) => Access(arr, index)
    }
  }
}
