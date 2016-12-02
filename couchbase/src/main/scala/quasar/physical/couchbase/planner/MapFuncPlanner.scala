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

package quasar.physical.couchbase.planner

import quasar.Predef._
import quasar.DataCodec, DataCodec.Precise.{DateKey, TimeKey, TimestampKey}
import quasar.{Data => QData, Type => QType, NameGenerator}
import quasar.contrib.matryoshka._
import quasar.fp._, eitherT._
import quasar.physical.couchbase._, N1QL.{Eq, Split, _}, Case._, Select.{Value, _}
import quasar.qscript, qscript.{MapFunc, MapFuncs => MF}
import quasar.std.StdLib.string.{dateRegex, timeRegex, timestampRegex}

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._

final class MapFuncPlanner[T[_[_]]: Recursive: Corecursive: ShowT, F[_]: Monad: NameGenerator]
  extends Planner[T, F, MapFunc[T, ?]] {

  def str(s: String)   = Data[T[N1QL]](QData.Str(s))
  def int(i: Int)      = Data[T[N1QL]](QData.Int(i))
  def bool(b: Boolean) = Data[T[N1QL]](QData.Bool(b))
  val na               = Data[T[N1QL]](QData.NA)

  def unwrap(a1: N1QLT[T]): N1QLT[T] =
    IfMissing(
      SelectField(a1.embed, str(DateKey).embed).embed,
      SelectField(a1.embed, str(TimeKey).embed).embed,
      SelectField(a1.embed, str(TimestampKey).embed).embed,
      a1.embed)

  def extract(a1: N1QLT[T], part: String): N1QLT[T] =
    DatePartStr(unwrap(a1).embed, str(part).embed)

  def rel(op: N1QLT[T]): N1QLT[T] = {
    def handleDates(a1: T[N1QL], a2: T[N1QL], o: (T[N1QL], T[N1QL]) => N1QLT[T]): N1QLT[T] = {
      val a1U = unwrap(a1.project).embed
      val a2U = unwrap(a2.project).embed
      Case(
        WhenThen(
          IsNotNull(IfMissing(
            SelectField(a1, str(DateKey).embed).embed,
            SelectField(a2, str(DateKey).embed).embed).embed).embed,
          o(DateDiffStr(a1U, a2U, str("day").embed).embed, int(0).embed).embed),
        WhenThen(
          IsNotNull(IfMissing(
            SelectField(a1, str(TimeKey).embed).embed,
            SelectField(a1, str(TimestampKey).embed).embed,
            SelectField(a2, str(TimeKey).embed).embed,
            SelectField(a2, str(TimestampKey).embed).embed).embed).embed,
          o(DateDiffStr(a1U, a2U, str("millisecond").embed).embed, int(0).embed).embed)
      )(
        Else(o(a1, a2).embed)
      )
    }

    op match {
      case Eq(a1, a2)              => handleDates(a1, a2, Eq(_, _))
      case Neq(a1, a2)             => handleDates(a1, a2, Neq(_, _))
      case Lt(a1, a2)              => handleDates(a1, a2, Lt(_, _))
      case Lte(a1, a2)             => handleDates(a1, a2, Lte(_, _))
      case Gt(a1, a2)              => handleDates(a1, a2, Gt(_, _))
      case Gte(a1, a2)             => handleDates(a1, a2, Gte(_, _))
      case v                       => v
    }
  }

  def datetime(a1: N1QLT[T], key: String, regex: Regex): N1QLT[T] =
    Case(
      WhenThen(
        IsNotNull(SelectField(a1.embed, str(key).embed).embed).embed,
        a1.embed),
      WhenThen(
        RegexContains(a1.embed, str(regex.regex).embed).embed,
        Obj(Map(str(key).embed -> a1.embed)).embed)
    )(
      Else(Null[T[N1QL]].embed)
    )

  def plan: AlgebraM[M, MapFunc[T, ?], N1QLT[T]] = {
    // nullary
    case MF.Constant(v) =>
      Data[T[N1QL]](v.cataM(QData.fromEJson)).ηM
    case MF.Undefined() =>
      Data[T[N1QL]](QData.NA).ηM

    // array
    case MF.Length(a1) =>
      IfMissingOrNull(
        Length(a1.embed).embed,
        LengthArr(a1.embed).embed,
        LengthObj(a1.embed).embed,
        na.embed
      ).ηM

    // date
    case MF.Date(a1) =>
      datetime(a1, DateKey, dateRegex.r).ηM
    case MF.Time(a1) =>
      datetime(a1, TimeKey, timeRegex.r).ηM
    case MF.Timestamp(a1) =>
      datetime(a1, TimestampKey, timestampRegex.r).ηM
    case MF.Interval(a1) =>
      unimplementedP("Interval")
    case MF.TimeOfDay(a1) =>
      def fracZero(a1: N1QLT[T]): N1QLT[T] =
        Case(
          WhenThen(
            RegexContains(a1.embed, str("[.]").embed).embed,
            Time(a1.embed).embed),
          WhenThen(
            IsNotNull(a1.embed).embed,
            Time(ConcatStr(a1.embed, str(".000").embed).embed).embed)
        )(
          Else(na.embed)
        )

      def timeFromTS(a1: N1QLT[T]): N1QLT[T] =
        Time(SelectElem(
          Split(
            SelectElem(
              Split(SelectField(a1.embed, str(TimestampKey).embed).embed,  str("T").embed).embed,
              int(1).embed).embed,
            str("Z").embed).embed,
          int(0).embed).embed)

      Case(
        WhenThen(SelectField(a1.embed, str(DateKey).embed).embed, na.embed),
        WhenThen(SelectField(a1.embed, str(TimeKey).embed).embed, a1.embed),
        WhenThen(SelectField(a1.embed, str(TimestampKey).embed).embed, timeFromTS(a1).embed)
      )(
        Else(fracZero(MillisToUTC(Millis(a1.embed).embed, str("00:00:00.000").embed.some)).embed)
      ).ηM
    case MF.ToTimestamp(a1) =>
      Timestamp(MillisToUTC(a1.embed, none).embed).ηM
    case MF.ExtractCentury(a1) =>
      Ceil(Div(extract(a1, "year").embed, int(100).embed).embed).ηM
    case MF.ExtractDayOfMonth(a1) =>
      extract(a1, "day").ηM
    case MF.ExtractDecade(a1)         =>
      extract(a1, "decade").ηM
    case MF.ExtractDayOfWeek(a1)      =>
      extract(a1, "day_of_week").ηM
    case MF.ExtractDayOfYear(a1)      =>
      extract(a1, "day_of_year").ηM
    case MF.ExtractEpoch(a1) =>
      Div(
        Millis(
          Case(
            WhenThen(
              SelectField(a1.embed, str(DateKey).embed).embed,
              ConcatStr(
                SelectField(a1.embed, str(DateKey).embed).embed,
                str("T00:00:00.000Z").embed).embed),
            WhenThen(
              SelectField(a1.embed, str(TimeKey).embed).embed,
              na.embed)
          )(
            Else(IfMissing(
              SelectField(a1.embed, str(TimestampKey).embed).embed,
              a1.embed).embed)
          ).embed
        ).embed,
        int(1000).embed
      ).ηM
    case MF.ExtractHour(a1) =>
      extract(a1, "hour").ηM
    case MF.ExtractIsoDayOfWeek(a1) =>
      extract(a1, "iso_dow").ηM
    case MF.ExtractIsoYear(a1)        =>
      extract(a1, "iso_year").ηM
    case MF.ExtractMicroseconds(a1) =>
      Mult(
        Add(
          Mult(extract(a1, "second").embed, int(1000).embed).embed,
          extract(a1, "millisecond").embed).embed,
        int(1000).embed
      ).ηM
    case MF.ExtractMillennium(a1) =>
      Ceil(Div(extract(a1, "year").embed, int(1000).embed).embed).ηM
    case MF.ExtractMilliseconds(a1) =>
      Add(
        Mult(extract(a1, "second").embed, int(1000).embed).embed,
        extract(a1, "millisecond").embed
      ).ηM
    case MF.ExtractMinute(a1) =>
      extract(a1, "minute").ηM
    case MF.ExtractMonth(a1) =>
      extract(a1, "month").ηM
    case MF.ExtractQuarter(a1) =>
      extract(a1, "quarter").ηM
    case MF.ExtractSecond(a1) =>
      Add(
        extract(a1, "second").embed,
        Div(extract(a1, "millisecond").embed, int(1000).embed).embed
      ).ηM
    case MF.ExtractTimezone(a1) =>
      extract(a1, "timezone").ηM
    case MF.ExtractTimezoneHour(a1) =>
      extract(a1, "timezone_hour").ηM
    case MF.ExtractTimezoneMinute(a1) =>
      extract(a1, "timezone_minute").ηM
    case MF.ExtractWeek(a1) =>
      extract(a1, "iso_week").ηM
    case MF.ExtractYear(a1) =>
      extract(a1, "year").ηM
    case MF.Now() =>
      NowStr[T[N1QL]].ηM

    // math
    case MF.Negate(a1) =>
      Neg(a1.embed).ηM
    case MF.Add(a1, a2) =>
      Add(a1.embed, a2.embed).ηM
    case MF.Multiply(a1, a2) =>
      Mult(a1.embed, a2.embed).ηM
    case MF.Subtract(a1, a2) =>
      Sub(a1.embed, a2.embed).ηM
    case MF.Divide(a1, a2) =>
      Div(a1.embed, a2.embed).ηM
    case MF.Modulo(a1, a2) =>
      Mod(a1.embed, a2.embed).ηM
    case MF.Power(a1, a2) =>
      Pow(a1.embed, a2.embed).ηM

    // relations
    case MF.Not(a1) =>
      Not(a1.embed).ηM
    case MF.Eq(a1, Data(QData.Null)) =>
      IsNull(unwrap(a1).embed).ηM
    case MF.Eq(a1, a2) =>
      rel(Eq(a1.embed, a2.embed)).ηM
    case MF.Neq(a1, Data(QData.Null)) =>
      IsNotNull(unwrap(a1).embed).ηM
    case MF.Neq(a1, a2) =>
      rel(Neq(a1.embed, a2.embed)).ηM
    case MF.Lt(a1, a2) =>
      rel(Lt(a1.embed, a2.embed)).ηM
    case MF.Lte(a1, a2) =>
      rel(Lte(a1.embed, a2.embed)).ηM
    case MF.Gt(a1, a2) =>
      rel(Gt(a1.embed, a2.embed)).ηM
    case MF.Gte(a1, a2) =>
      rel(Gte(a1.embed, a2.embed)).ηM
    case MF.IfUndefined(a1, a2) =>
      IfMissing(a1.embed, a2.embed).ηM
    case MF.And(a1, a2) =>
      And(a1.embed, a2.embed).ηM
    case MF.Or(a1, a2) =>
      Or(a1.embed, a2.embed).ηM
    case MF.Between(a1, a2, a3) =>
      And(rel(Gte(a1.embed, a2.embed)).embed, rel(Lte(a1.embed, a3.embed)).embed).ηM
    case MF.Cond(cond, then_, else_) =>
      Case(
        WhenThen(cond.embed, then_.embed)
      )(
        Else(else_.embed)
      ).ηM

    // set
    case MF.Within(a1, a2) =>
      ArrContains(a2.embed, a1.embed).ηM

    // string
    case MF.Lower(a1) =>
      Lower(a1.embed).ηM
    case MF.Upper(a1) =>
      Upper(a1.embed).ηM
    case MF.Bool(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1.embed).embed, str("true").embed).embed,
          bool(true).embed),
        WhenThen(
          Eq(Lower(a1.embed).embed, str("false").embed).embed,
          bool(false).embed)
      )(
        Else(na.embed)
      ).ηM
    // TODO: Handle large numbers across the board. Couchbase's number type truncates.
    case MF.Integer(a1) =>
      Case(
        WhenThen(
          Eq(
            ToNumber(a1.embed).embed,
            Floor(ToNumber(a1.embed).embed).embed).embed,
          ToNumber(a1.embed).embed)
      )(
        Else(na.embed)
      ).ηM
    case MF.Decimal(a1) =>
      ToNumber(a1.embed).ηM
    case MF.Null(a1) =>
      Case(
        WhenThen(
          Eq(Lower(a1.embed).embed, str("null").embed).embed,
          Null[T[N1QL]].embed)
      )(
        Else(na.embed)
      ).ηM
    case MF.ToString(a1) =>
      IfNull(
        ToString(a1.embed).embed,
        unwrap(a1).embed,
        Case(
          WhenThen(
            Eq(Type(a1.embed).embed, str("null").embed).embed ,
            str("null").embed)
        )(
          Else(a1.embed)
        ).embed
      ).ηM
    case MF.Search(a1, a2, a3)    =>
      Case(
        WhenThen(
          a3.embed,
          RegexContains(a1.embed, ConcatStr(str("(?i)(?s)").embed, a2.embed).embed).embed)
      )(
        Else(RegexContains(a1.embed, ConcatStr(str("(?s)").embed, a2.embed).embed).embed)
      ).ηM
    case MF.Substring(a1, a2, a3) =>
      Case(
        WhenThen(
          Lt(a2.embed, int(0).embed).embed,
          str("").embed),
        WhenThen(
          Lt(a3.embed, int(0).embed).embed,
          IfNull(
            Substr(a1.embed, a2.embed, None).embed,
            str("").embed).embed)
      )(
        Else(IfNull(
          Substr(
            a1.embed,
            a2.embed,
            Least(a3.embed, Sub(Length(a1.embed).embed, a2.embed).embed).embed.some
          ).embed,
          str("").embed).embed)
      ).ηM

    // structural
    case MF.MakeArray(a1) =>
      Arr(List(a1.embed)).ηM
    case MF.MakeMap(a1, a2: Select[T[N1QL]]) =>
      genId[T, M] ∘ (id1 =>
        Select(
          Value(true),
          ResultExpr(
            Obj(Map(
              ToString(a1.embed).embed -> IfNull(id1.embed, na.embed).embed
            )).embed,
            none
          ).wrapNel,
          Keyspace(a2.embed, id1.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil))
    case MF.MakeMap(a1, a2) =>
      Obj(Map(a1.embed -> a2.embed)).ηM
    case MF.ConcatArrays(a1, a2) =>
      IfNull(
        ConcatStr(a1.embed, a2.embed).embed,
        ConcatArr(
          Case(
            WhenThen(
              IsString(a1.embed).embed,
              Split(a1.embed, str("").embed).embed)
          )(
            Else(a1.embed)
          ).embed,
          Case(
            WhenThen(
              IsString(a2.embed).embed,
              Split(a2.embed, str("").embed).embed)
          )(
            Else(a2.embed)
          ).embed).embed
      ).ηM
    case MF.ConcatMaps(a1, a2) =>
      ConcatObj(a1.embed, a2.embed).ηM
    case MF.ProjectField(a1: Select[T[N1QL]], a2) =>
      genId[T, M] ∘ (id =>
        Select(
          Value(true),
          ResultExpr(SelectField(id.embed, a2.embed).embed, none).wrapNel,
          Keyspace(a1.embed, id.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil))
    case MF.ProjectField(a1, a2) =>
      SelectField(a1.embed, a2.embed).ηM
    case MF.ProjectIndex(a1: Select[T[N1QL]], a2) =>
      genId[T, M] ∘ (id =>
        Select(
          Value(true),
          ResultExpr(SelectElem(id.embed, a2.embed).embed, none).wrapNel,
          Keyspace(a1.embed, id.some).some,
          unnest  = none,
          filter  = none,
          groupBy = none,
          orderBy = Nil))
    case MF.ProjectIndex(a1, a2) =>
      SelectElem(a1.embed, a2.embed).ηM
    case MF.DeleteField(a1, a2) =>
      ObjRemove(a1.embed, a2.embed).ηM

    // helpers & QScript-specific
    case MF.Range(a1, a2) =>
      Slice(a2.embed, Add(a1.embed, int(1).embed).embed.some).ηM
    case MF.Guard(expr, typ, cont, _) =>
      def grd(f: N1QLT[T] => N1QLT[T], e: N1QLT[T], c: N1QLT[T]): N1QLT[T] =
        Case(
          WhenThen(f(e).embed, c.embed))(
          Else(na.embed))

      def grdSel(f: N1QLT[T] => N1QLT[T]): M[N1QLT[T]] =
        genId[T, M] ∘ (id =>
          Select(
            Value(true),
            ResultExpr(grd(f, id, id).embed, none).wrapNel,
            Keyspace(cont.embed, id.some).some,
            unnest  = none,
            filter  = none,
            groupBy = none,
            orderBy = Nil))

      def isArr(n: N1QLT[T]): N1QLT[T] = IsArr(n.embed)
      def isObj(n: N1QLT[T]): N1QLT[T] = IsObj(n.embed)

      (cont, typ) match {
        case (_: Select[T[N1QL]], _: QType.FlexArr) => grdSel(isArr)
        case (_: Select[T[N1QL]], _: QType.Obj)     => grdSel(isObj)
        case (_                 , _: QType.FlexArr) => grd(isArr, expr, cont).point[M]
        case (_                 , _: QType.Obj)     => grd(isObj, expr, cont).point[M]
        case _                                      => cont.point[M]
      }
  }

}
