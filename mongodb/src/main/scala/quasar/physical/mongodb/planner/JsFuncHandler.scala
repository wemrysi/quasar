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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.{Data, Planner, Type}, Planner._, Type.{Coproduct => _, _}
import quasar.ejson.EJson
import quasar.fp._
import quasar.fp.ski._
import quasar.fs.MonadFsErr
import quasar.javascript.Js
import quasar.jscore, jscore.{JsCore, JsCoreF, Name}
import quasar.physical.mongodb.planner.common._
import quasar.qscript._
import quasar.std.StdLib._
import quasar.std.TemporalPart._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{Divide => _, _}, Scalaz._

trait JsFuncHandler[IN[_]] {
  def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, IN, JsCore]
}

object JsFuncHandler {

  // TODO: Use `JsonCodec.encode` and avoid failing.
  private def ejsonToJs[M[_]: Applicative: MonadFsErr, EJ: Show]
    (ej: EJ)(implicit EJ: Recursive.Aux[EJ, EJson])
      : M[JsCore] =
    ej.cata(Data.fromEJson).toJs.fold(
      raisePlannerError[M, JsCore](NonRepresentableEJson(ej.shows)))(
      _.point[M])

  implicit def mapFuncCore[T[_[_]]: BirecursiveT: ShowT, J, E]
      : JsFuncHandler[MapFuncCore[T, ?]] =
    new JsFuncHandler[MapFuncCore[T, ?]] {
      import jscore.{
        Add => _, In => _,
        Lt => _, Lte => _, Gt => _, Gte => _, Eq => _, Neq => _,
        And => _, Or => _, Not => _, TypeOf => _,
        _}

      val mjs = quasar.physical.mongodb.javascript[JsCore](_.embed)
      import mjs._

      import MapFuncsCore._

      // NB: Math.trunc is not present in MongoDB.
      private def trunc(expr: JsCore): JsCore =
        Let(Name("x"), expr,
          BinOp(jscore.Sub,
            ident("x"),
            BinOp(jscore.Mod, ident("x"), Literal(Js.Num(1, false)))))

      private def execTime[M[_]: Applicative](implicit ev: ExecTimeR[M]): M[JsCore] =
          ev.ask map (ts => Literal(Js.Str(ts.toString)))

      def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, MapFuncCore[T, ?], JsCore] =
        mf => handleCommon(mf).cata(_.point[M], handleSpecial[M].apply(mf))

      def handleCommon(mf: MapFuncCore[T, JsCore]): Option[JsCore] =
        handle0.apply(mf).map(unpack[Fix, JsCoreF])

      def year(date: JsCore): JsCore =
        Call(Select(date, "getUTCFullYear"), Nil)

      def month(date: JsCore): JsCore =
        BinOp(jscore.Add,
          Call(Select(date, "getUTCMonth"), Nil),
          litNum(1))

      def day(date: JsCore): JsCore =
        Call(Select(date, "getUTCDate"), Nil)

      def hour(date: JsCore): JsCore =
        Call(Select(date, "getUTCHours"), Nil)

      def minute(date: JsCore): JsCore =
        Call(Select(date, "getUTCMinutes"), Nil)

      def second(date: JsCore): JsCore =
        Call(Select(date, "getUTCSeconds"), Nil)

      def millisecond(date: JsCore): JsCore =
        Call(Select(date, "getUTCMilliseconds"), Nil)

      def dayOfWeek(date: JsCore): JsCore =
        Call(Select(date, "getUTCDay"), Nil)

      def quarter(date: JsCore): JsCore =
        BinOp(jscore.Add,
          Call(Select(ident("Math"), "floor"), List(
            BinOp(jscore.Div, Call(Select(date, "getUTCMonth"), Nil), litNum(3)))),
          litNum(1))

      def decade(date: JsCore): JsCore =
        trunc(BinOp(jscore.Div, year(date), litNum(10)))

      def century(date: JsCore): JsCore =
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div, year(date), litNum(100))))

      def millennium(date: JsCore): JsCore =
        Call(Select(ident("Math"), "ceil"), List(
          BinOp(jscore.Div, year(date), litNum(1000))))

      def litStr(s: String): JsCore =
        Literal(Js.Str(s))

      def litNum(i: Int): JsCore =
        Literal(Js.Num(i.toDouble, false))

      def handleSpecial[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, MapFuncCore[T, ?], JsCore] = {
        case Constant(v1) => ejsonToJs[M, T[EJson]](v1)
        case JoinSideName(n) => raisePlannerError[M, JsCore](UnexpectedJoinSide(n))
        case Now() => execTime[M] map (ts => New(Name("ISODate"), List(ts)))
        case Interval(a1) => unimplemented[M, JsCore]("Interval JS")

        // TODO: De-duplicate and move these to JsFuncHandler
        case ExtractCentury(date) =>
          Call(ident("NumberLong"), List(
            century(date))).point[M]
        case ExtractDayOfMonth(date) =>
          day(date).point[M]
        case ExtractDecade(date) =>
          Call(ident("NumberLong"), List(decade(date))).point[M]
        case ExtractDayOfWeek(date) =>
          dayOfWeek(date).point[M]
        case ExtractDayOfYear(date) =>
          Call(ident("NumberInt"), List(
            Call(Select(ident("Math"), "floor"), List(
              BinOp(jscore.Add,
                BinOp(jscore.Div,
                  BinOp(Sub,
                    date,
                    New(Name("Date"), List(
                      Call(Select(date, "getFullYear"), Nil),
                      Literal(Js.Num(0, false)),
                      Literal(Js.Num(0, false))))),
                  Literal(Js.Num(86400000, false))),
                Literal(Js.Num(1, false))))))).point[M]
        case ExtractEpoch(date) =>
          Call(ident("NumberLong"), List(
            BinOp(jscore.Div,
              Call(Select(date, "valueOf"), Nil),
              litNum(1000)))).point[M]
        case ExtractHour(date) =>
          hour(date).point[M]
        case ExtractIsoDayOfWeek(date) =>
          Let(Name("x"), dayOfWeek(date),
            If(
              BinOp(jscore.Eq, ident("x"), litNum(0)),
              litNum(7),
              ident("x"))).point[M]
        case ExtractIsoYear(date) =>
          year(date).point[M]
        case ExtractMicroseconds(date) =>
          BinOp(jscore.Mult,
            BinOp(jscore.Add,
              millisecond(date),
              BinOp(jscore.Mult,
                second(date),
                litNum(1000))),
            litNum(1000)).point[M]
        case ExtractMillennium(date) =>
          Call(ident("NumberLong"), List(millennium(date))).point[M]
        case ExtractMilliseconds(date) =>
          BinOp(jscore.Add,
            millisecond(date),
            BinOp(jscore.Mult,
              second(date),
              litNum(1000))).point[M]
        case ExtractMinute(date) =>
          minute(date).point[M]
        case ExtractMonth(date) =>
          month(date).point[M]
        case ExtractQuarter(date) =>
          Call(ident("NumberInt"), List(quarter(date))).point[M]
        case ExtractSecond(date) =>
          BinOp(jscore.Add,
            second(date),
            BinOp(jscore.Div, millisecond(date), litNum(1000))).point[M]
        case ExtractWeek(date) =>
          Call(ident("NumberInt"), List(
            Call(Select(ident("Math"), "floor"), List(
              BinOp(jscore.Add,
                BinOp(jscore.Div,
                  Let(Name("startOfYear"),
                    New(Name("Date"), List(
                      Call(Select(date, "getFullYear"), Nil),
                      Literal(Js.Num(0, false)),
                      Literal(Js.Num(1, false)))),
                    BinOp(jscore.Add,
                      BinOp(Div,
                        BinOp(Sub, date, ident("startOfYear")),
                        Literal(Js.Num(86400000, false))),
                      BinOp(jscore.Add,
                        Call(Select(ident("startOfYear"), "getDay"), Nil),
                        Literal(Js.Num(1, false))))),
                  Literal(Js.Num(7, false))),
                Literal(Js.Num(1, false))))))).point[M]

        case MakeMap(Embed(LiteralF(Js.Str(str))), a2) => Obj(ListMap(Name(str) -> a2)).point[M]
        // TODO: pull out the literal, and handle this case in other situations
        case MakeMap(a1, a2) => Obj(ListMap(Name("__Quasar_non_string_map") ->
         Arr(List(Arr(List(a1, a2)))))).point[M]
        case ConcatArrays(Embed(ArrF(a1)), Embed(ArrF(a2))) =>
          Arr(a1 |+| a2).point[M]
        case ConcatArrays(a1, a2) =>
          If(BinOp(jscore.Or, isArray(a1), isArray(a2)),
            Call(Select(a1, "concat"), List(a2)),
            BinOp(jscore.Add, a1, a2)).point[M]
        case ConcatMaps(Embed(ObjF(o1)), Embed(ObjF(o2))) =>
          Obj(o1 ++ o2).point[M]
        case ConcatMaps(a1, a2) => SpliceObjects(List(a1, a2)).point[M]

        case Guard(expr, typ, cont, fallback) =>
          val jsCheck: Type => Option[JsCore => JsCore] =
            generateTypeCheck[JsCore, JsCore](BinOp(jscore.Or, _, _)) {
              case Type.Null             => isNull
              case Type.Dec              => isDec
              case Type.Int
                 | Type.Int ⨿ Type.Dec
                 | Type.Int ⨿ Type.Dec ⨿ Type.Interval
                  => isAnyNumber
              case Type.Str              => isString
              case Type.Obj(_, _) ⨿ Type.FlexArr(_, _, _)
                  => isObjectOrArray
              case Type.Obj(_, _)        => isObject
              case Type.FlexArr(_, _, _) => isArray
              case Type.Binary           => isBinary
              case Type.Id               => isObjectId
              case Type.Bool             => isBoolean
              case Type.Date             => isDate
              case Type.Syntaxed         => isSyntaxed
            }
          jsCheck(typ).fold[M[JsCore]](
            raiseInternalError("uncheckable type"))(
            f => If(f(expr), cont, fallback).point[M])

        // TODO: Specify the function name for pattern match failures
        case _ => unimplemented[M, JsCore]("JS function")
      }

      def handle0: MapFuncCore[T, ?] ~> OptionFree[JsCoreF, ?] =
        new (MapFuncCore[T, ?] ~> OptionFree[JsCoreF, ?]) {

          def apply[A](mfc: MapFuncCore[T, A]): OptionFree[JsCoreF, A] = {
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

            def partial(mfc: MapFuncCore[T, A]): OptionFree[JsCoreF, A] = mfc.some collect {
              case Add(a1, a2)      => BinOp(jscore.Add, a1, a2)
              case Multiply(a1, a2) => BinOp(jscore.Mult, a1, a2)
              case Power(a1, a2) =>
                Call(select(ident("Math"), "pow"), List(a1, a2))
              case Subtract(a1, a2) => BinOp(jscore.Sub, a1, a2)
              case Divide(a1, a2)   => BinOp(jscore.Div, a1, a2)
              case Modulo(a1, a2)   => BinOp(jscore.Mod, a1, a2)
              case Negate(a1)       => UnOp(jscore.Neg, a1)
              case Undefined()      => ident("undefined")

              case MapFuncsCore.Eq(a1, a2)  => BinOp(jscore.Eq, a1, a2)
              case Neq(a1, a2) => BinOp(jscore.Neq, a1, a2)
              case Lt(a1, a2)  => BinOp(jscore.Lt, a1, a2)
              case Lte(a1, a2) => BinOp(jscore.Lte, a1, a2)
              case Gt(a1, a2)  => BinOp(jscore.Gt, a1, a2)
              case Gte(a1, a2) => BinOp(jscore.Gte, a1, a2)
              case Not(a1)     => UnOp(jscore.Not, a1)
              case And(a1, a2) => BinOp(jscore.And, a1, a2)
              case Or(a1, a2)  => BinOp(jscore.Or, a1, a2)
              case Between(value, min, max) =>
                  BinOp(jscore.And,
                    BinOp(jscore.Lte, min, value),
                    BinOp(jscore.Lte, value, max))

              case MakeArray(a1) => Arr(List(a1))
              case Length(str) =>
                Call(ident("NumberLong"), List(select(hole(str), "length")))
              case Substring(field, start, len) =>
                If(BinOp(jscore.Lt, start, litNum(0)),
                  litStr(""),
                  If(BinOp(jscore.Lt, len, litNum(0)),
                    Call(select(field, "substr"), List(start, select(field, "length"))),
                    Call(select(field, "substr"), List(start, len))))
              case MapFuncsCore.Split(a1, a2) =>
                Call(select(a1, "split"), List(a2))
              case Lower(a1) => Call(select(a1, "toLowerCase"), Nil)
              case Upper(a1) => Call(select(a1, "toUpperCase"), Nil)
              case Search(field, pattern, insen) =>
                  Call(
                    select(
                      New(Name("RegExp"), List(
                        pattern,
                        If(insen, litStr("im"), litStr("m")))),
                      "test"),
                    List(field))
              case Within(a1, a2) =>
                BinOp(jscore.Neq,
                  Literal(Js.Num(-1, false)),
                  Call(select(a2, "indexOf"), List(a1)))
              case Null(str) =>
                If(
                  BinOp(jscore.Eq, str, litStr("null")),
                  Literal(Js.Null),
                  ident("undefined"))
              case Bool(str) =>
                If(
                  BinOp(jscore.Eq, str, litStr("true")),
                  Literal(Js.Bool(true)),
                  If(
                    BinOp(jscore.Eq, str, litStr("false")),
                    Literal(Js.Bool(false)),
                    ident("undefined")))
              case Integer(str) =>
                If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.intRegex + "$"))), "test"), List(str)),
                  Call(ident("NumberLong"), List(str)),
                  ident("undefined"))
              case Decimal(str) =>
                  If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.floatRegex + "$"))), "test"), List(str)),
                    Call(ident("parseFloat"), List(str)),
                    ident("undefined"))
              case Date(str) =>
                If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.dateRegex + "$"))), "test"), List(str)),
                  Call(ident("ISODate"), List(str)),
                  ident("undefined"))
              case Time(str) =>
                If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.timeRegex + "$"))), "test"), List(str)),
                  str,
                  ident("undefined"))
              case Timestamp(str) =>
                If(Call(select(Call(ident("RegExp"), List(litStr("^" + string.timestampRegex + "$"))), "test"), List(str)),
                  Call(ident("ISODate"), List(str)),
                  ident("undefined"))
              // TODO: case Interval(str) =>

              case ToString(value) =>
                If(isInt(value),
                  // NB: This is a terrible way to turn an int into a string, but the
                  //     only one that doesn’t involve converting to a decimal and
                  //     losing precision.
                  Call(select(Call(ident("String"), List(value)), "replace"), List(
                    Call(ident("RegExp"), List(
                      litStr("[^-0-9]+"),
                      litStr("g"))),
                    litStr(""))),
                  If(isObjectId(value),
                    Call(select(value, "toString"), Nil),
                    (If(binop(jscore.Or, isTimestamp(value), isDate(value)),
                      Call(select(value, "toISOString"), Nil),
                      Call(ident("String"), List(value))))))

              case ToTimestamp(a1) => New(Name("Date"), List(a1))
              case TimeOfDay(date) =>
                Let(Name("t"), date,
                  binop(jscore.Add,
                    pad2(hour(ident("t"))),
                    litStr(":"),
                    pad2(minute(ident("t"))),
                    litStr(":"),
                    pad2(second(ident("t"))),
                    litStr("."),
                    pad3(millisecond(ident("t")))))

              case ExtractYear(date) => year(date)

              case StartOfDay(date) =>
                dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0))

              case TemporalTrunc(Century, date) =>
                val yr =
                  Call(select(ident("Math"), "floor"), List(BinOp(jscore.Div, year(date), litNum(100))))
                dateZ(
                  BinOp(jscore.Mult, yr, litNum(100)),
                  litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Day, date) =>
                dateZ(year(date), month(date), day(date), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Decade, date) =>
                dateZ(
                  BinOp(jscore.Mult, decade(date), litNum(10)),
                  litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Hour, date) =>
                dateZ(year(date), month(date), day(date), hour(date), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Millennium, date) =>
                dateZ(
                  BinOp(jscore.Mult,
                    Call(select(ident("Math"), "floor"), List(
                      BinOp(jscore.Div, year(date), litNum(1000)))),
                    litNum(1000)),
                  litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Microsecond | Millisecond, date) =>
                dateZ(
                  year(date), month(date), day(date),
                  hour(date), minute(date), second(date), millisecond(date))
              case TemporalTrunc(Minute, date) =>
                dateZ(year(date), month(date), day(date), hour(date), minute(date), litNum(0), litNum(0))
              case TemporalTrunc(Month, date) =>
                dateZ(year(date), month(date), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Quarter, date) =>
                dateZ(
                  year(date),
                  BinOp(jscore.Add,
                    BinOp(jscore.Mult,
                      BinOp(jscore.Sub, quarter(date), litNum(1)),
                      litNum(3)),
                    litNum(1)),
                  litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))
              case TemporalTrunc(Second, date) =>
                dateZ(year(date), month(date), day(date), hour(date), minute(date), second(date), litNum(0))
              case TemporalTrunc(Week, date) =>
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
              case TemporalTrunc(Year, date) =>
                dateZ(year(date), litNum(1), litNum(1), litNum(0), litNum(0), litNum(0), litNum(0))

              case ProjectKey(obj, key) => Access(obj, key)
              case ProjectIndex(arr, index) => Access(arr, index)
              case DeleteKey(a1, a2) => Call(ident("remove"), List(a1, a2))

              // TODO: This doesn't return the right values most of the time.
              case TypeOf(v) =>
                Let(Name("typ"), UnOp(jscore.TypeOf, v),
                  If(BinOp(jscore.Eq, ident("typ"), Literal(Js.Str("object"))),
                    If(BinOp(jscore.Eq, v, Literal(Js.Null)),
                      Literal(Js.Str("null")),
                      If(Call(select(ident("Array"), "isArray"), List(v)),
                        Literal(Js.Str("array")),
                        Literal(Js.Str("map")))),
                    If(BinOp(jscore.Eq, ident("typ"), Literal(Js.Str("string"))),
                      Literal(Js.Str("array")),
                      ident("typ"))))

              case IfUndefined(a1, a2) =>
                // TODO: Only evaluate `value` once.
                If(BinOp(jscore.Eq, a1, ident("undefined")), a2, a1)

              case ToId(a1) => New(Name("ObjectId"), List(a1))
              case Cond(i, t, e) => If(i, t, e)

              // FIXME: Doesn't work for Char.
              case Range(start, end) =>
                Call(
                  select(
                    Call(select(ident("Array"), "apply"), List(
                      Literal(Js.Null),
                      Call(ident("Array"), List(BinOp(jscore.Sub, end, start))))),
                    "map"),
                  List(
                    Fun(List(Name("element"), Name("index")),
                      BinOp(jscore.Add, ident("index"), start))))

            }

            partial(mfc) orElse (mfc match {
              case _                => None
            })
          }
        }
    }

  implicit def mapFuncDerived[T[_[_]]: CorecursiveT]
    (implicit core: JsFuncHandler[MapFuncCore[T, ?]])
      : JsFuncHandler[MapFuncDerived[T, ?]] =
    new JsFuncHandler[MapFuncDerived[T, ?]] {
      def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, MapFuncDerived[T, ?], JsCore] =
        ExpandMapFunc.expand(core.handle[M], κ[MapFuncDerived[T, JsCore], Option[M[JsCore]]](None))
    }

  implicit def mapFuncCoproduct[F[_], G[_]]
      (implicit F: JsFuncHandler[F], G: JsFuncHandler[G])
      : JsFuncHandler[Coproduct[F, G, ?]] =
    new JsFuncHandler[Coproduct[F, G, ?]] {
      def handle[M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, Coproduct[F, G, ?], JsCore] =
        _.run.fold(F.handle[M], G.handle[M])
    }

  def handle[F[_]: JsFuncHandler, M[_]: Monad: MonadFsErr: ExecTimeR]: AlgebraM[M, F, JsCore] =
    implicitly[JsFuncHandler[F]].handle[M]
}
