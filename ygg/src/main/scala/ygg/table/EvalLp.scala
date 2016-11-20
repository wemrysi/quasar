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

package ygg.table

import ygg._, common._
import quasar._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}
import quasar.{ qscript => qs }
import quasar.qscript.{ MapFuncs => mf }

abstract class EvalLp[A] extends quasar.qscript.TTypes[Fix] {
  def undef: A
  def read(path: FPath): A
  def constant(data: Data): A
  def bind(let: Sym, form: A, in: A): A
  def typecheck(expr: A, typ: Type, ifp: A, elsep: A): A
  def free(sym: Sym): A

  def mapFuncStep: Algebra[MapFunc, A]

  def lplanStep: Algebra[LP, A] = {
    case lp.Read(path)                           => read(path)
    case lp.Constant(data)                       => constant(data)
    case lp.Free(sym)                            => free(sym)
    case lp.Let(let, form, in)                   => bind(let, form, in)
    case lp.Typecheck(expr, typ, cont, fallback) => typecheck(expr, typ, cont, fallback)
    case UnaryMF(f, a1)                          => mapFuncStep(qs.MapFunc.translateUnaryMapping(f)(a1))
    case BinaryMF(f, a1, a2)                     => mapFuncStep(qs.MapFunc.translateBinaryMapping(f)(a1, a2))
    case TernaryMF(f, a1, a2, a3)                => mapFuncStep(qs.MapFunc.translateTernaryMapping(f)(a1, a2, a3))
  }
  def eval(lp: Fix[LP]): A = {
    println(lp.render.draw mkString "\n")
    lp cata lplanStep
  }
}

object EvalLp extends quasar.qscript.TTypes[Fix] {
  def apply[A: TableRep](files: Map[FPath, A], args: Sym => A): EvalLp[A] = new Impl[A](files, args)

  sealed trait LpAst[A]
  object LpAst {
    final case class Table[A: TableRep](table: A) extends LpAst[A]
    final case class Free[A](sym: A) extends LpAst[A]
  }

  class Impl[A: TableRep](files: Map[FPath, A], args: Sym => A) extends EvalLp[A] {
    val C = companionOf[A]
    import C._

    def free(sym: Sym): A                                  = args(sym)
    def undef: A                                           = ???
    def bind(let: Sym, form: A, in: A): A                  = ???
    def typecheck(expr: A, typ: Type, ifp: A, elsep: A): A = ???
    def read(path: FPath): A                               = files(path)

    def constant(data: Data): A = data match {
      case Data.Int(x)  => constLong(x.longValue)
      case Data.Dec(x)  => constDouble(x.doubleValue)
      case Data.Str(x)  => constString(x)
      case Data.Bool(x) => constBoolean(x)
      case Data.Null    => constNull
    }

    val mapFuncStep: Algebra[MapFunc, A] = {
      case mf.Add(x, y)                   => ??? // binOp[F](x, y)(_ + _)
      case mf.And(x, y)                   => ??? // binOp[F](x, y)(_ and _)
      case mf.Between(v1, v2, v3)         => ??? // ternOp[F](v1, v2, v3)((x1, x2, x3) => mkSeq_(x2 le x1) and mkSeq_(x1 le x3))
      case mf.Bool(s)                     => ??? // xs.boolean(s).point[F]
      case mf.ConcatArrays(x, y)          => ??? // ejson.arrayConcat[F] apply (x, y)
      case mf.ConcatMaps(x, y)            => ??? // ejson.objectConcat[F] apply (x, y)
      case mf.Cond(p, t, f)               => ??? // if_(p).then_(t).else_(f).point[F]
      case mf.Constant(ejson)             => ???
      case mf.Date(s)                     => ??? // qscript.asDate[F] apply s
      case mf.Decimal(s)                  => ??? // xs.double(s).point[F]
      case mf.DeleteField(src, field)     => ???
      case mf.Divide(x, y)                => ??? // binOp[F](x, y)(_ div _)
      case mf.Eq(x, y)                    => ??? // binOp[F](x, y)(_ eq _)
      case mf.ExtractCentury(time)        => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractDayOfMonth(time)     => ??? // qscript.asDateTime[F] apply time map fn.dayFromDateTime
      case mf.ExtractDayOfWeek(time)      => ??? // qscript.asDate[F].apply(time) map (d => mkSeq_(xdmp.weekdayFromDate(d) mod 7.xqy))
      case mf.ExtractDayOfYear(time)      => ??? // qscript.asDate[F].apply(time) map (xdmp.yeardayFromDate)
      case mf.ExtractDecade(time)         => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractEpoch(time)          => ??? // qscript.asDateTime[F] apply time flatMap (qscript.secondsSinceEpoch[F].apply(_))
      case mf.ExtractHour(time)           => ??? // qscript.asDateTime[F] apply time map fn.hoursFromDateTime
      case mf.ExtractIsoDayOfWeek(time)   => ??? // qscript.asDate[F].apply(time) map (xdmp.weekdayFromDate)
      case mf.ExtractIsoYear(time)        => ??? // qscript.asDateTime[F] apply time flatMap (qscript.isoyearFromDateTime[F].apply(_))
      case mf.ExtractMicroseconds(time)   => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractMillennium(time)     => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractMilliseconds(time)   => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractMinute(time)         => ??? // qscript.asDateTime[F] apply time map fn.minutesFromDateTime
      case mf.ExtractMonth(time)          => ??? // qscript.asDateTime[F] apply time map fn.monthFromDateTime
      case mf.ExtractQuarter(time)        => ??? // qscript.asDate[F].apply(time) map (xdmp.quarterFromDate)
      case mf.ExtractSecond(time)         => ??? // qscript.asDateTime[F] apply time map fn.secondsFromDateTime
      case mf.ExtractTimezone(time)       => ??? // qscript.asDateTime[F] apply time flatMap (qscript.timezoneOffsetSeconds[F].apply(_))
      case mf.ExtractTimezoneHour(time)   => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractTimezoneMinute(time) => ??? // qscript.asDateTime[F] apply time map (dt =>
      case mf.ExtractWeek(time)           => ??? // qscript.asDate[F].apply(time) map (xdmp.weekFromDate)
      case mf.ExtractYear(time)           => ??? // qscript.asDateTime[F] apply time map fn.yearFromDateTime
      case mf.Gt(x, y)                    => ??? // binOp[F](x, y)(_ gt _)
      case mf.Gte(x, y)                   => ??? // binOp[F](x, y)(_ ge _)
      case mf.Guard(_, _, cont, _)        => ??? // s"(: GUARD CONT :)$cont".xqy.point[F]
      case mf.IfUndefined(x, alternate)   => ??? // if_(fn.empty(x)).then_(alternate).else_(x).point[F]
      case mf.Integer(s)                  => ??? // xs.integer(s).point[F]
      case mf.Interval(s)                 => ??? // xs.dayTimeDuration(s).point[F]
      case mf.Length(arrOrstr)            => ??? // qscript.length[F] apply arrOrstr
      case mf.Lower(s)                    => ??? // fn.lowerCase(s).point[F]
      case mf.Lt(x, y)                    => ??? // binOp[F](x, y)(_ lt _)
      case mf.Lte(x, y)                   => ??? // binOp[F](x, y)(_ le _)
      case mf.MakeArray(x)                => ??? // ejson.singletonArray[F] apply x
      case mf.MakeMap(k, v)               => ???
      case mf.Modulo(x, y)                => ??? // binOp[F](x, y)(_ mod _)
      case mf.Multiply(x, y)              => ??? // binOp[F](x, y)(_ * _)
      case mf.Negate(x)                   => ??? // (-x).point[F]
      case mf.Neq(x, y)                   => ??? // binOp[F](x, y)(_ ne _)
      case mf.Not(x)                      => ??? // fn.not(x).point[F]
      case mf.Now()                       => ??? // fn.currentDateTime.point[F]
      case mf.Null(s)                     => ??? // (ejson.null_[F] |@| qscript.qError[F](s"Invalid coercion to 'null': $s".xs))(
      case mf.Or(x, y)                    => ??? // binOp[F](x, y)(_ or _)
      case mf.Power(b, e)                 => ??? // math.pow(b, e).point[F]
      case mf.ProjectField(src, field)    => ???
      case mf.ProjectIndex(arr, idx)      => ??? // ejson.arrayElementAt[F] apply (arr, idx + 1.xqy)
      case mf.Range(x, y)                 => ??? // (x to y).point[F]
      case mf.Search(in, ptn, ci)         => ??? // fn.matches(in, ptn, Some(if_ (ci) then_ "i".xs else_ "".xs)).point[F]
      case mf.Substring(s, loc, len)      => ??? // fn.substring(s, loc + 1.xqy, some(len)).point[F]
      case mf.Subtract(x, y)              => ??? // binOp[F](x, y)(_ - _)
      case mf.Time(s)                     => ??? // xs.time(s).point[F]
      case mf.TimeOfDay(dt)               => ??? // qscript.asDateTime[F] apply dt map xs.time
      case mf.Timestamp(s)                => ??? // xs.dateTime(s).point[F]
      case mf.ToString(x)                 => ??? // qscript.toString[F] apply x
      case mf.ToTimestamp(millis)         => ??? // qscript.timestampToDateTime[F] apply millis
      case mf.Undefined()                 => ??? // emptySeq.point[F]
      case mf.Upper(s)                    => ??? // fn.upperCase(s).point[F]
      case mf.Within(x, arr)              => ??? // qscript.elementLeftShift[F].apply(arr) map (xs => fn.exists(fn.indexOf(xs, x)))
    }
  }
}
