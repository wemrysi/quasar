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

package quasar.physical.sparkcore.fs

import quasar.Predef.{Eq => _, _}
import quasar.Data
import quasar.Planner._
import quasar.contrib.matryoshka._
import quasar.fp.ski._
import quasar.qscript._, MapFuncs._
import quasar.std.{DateLib, StringLib}

import scala.math

import matryoshka.{Hole => _, _}, Recursive.ops._
import org.threeten.bp.{Instant, ZoneOffset}
import scalaz.{Divide => _, _}, Scalaz._

object CoreMap extends Serializable {

  private val undefined = Data.NA

  def changeFreeMap[T[_[_]]: Recursive](f: FreeMap[T])
      : PlannerError \/ (Data => Data) =
    freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), change[T, Data]))

  def changeJoinFunc[T[_[_]]: Recursive](f: JoinFunc[T])
      : PlannerError \/ ((Data, Data) => Data) =
    freeCataM(f)(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, ((Data, Data)) => Data](
      (js: JoinSide) => (js match {
        case LeftSide  => (_: (Data, Data))._1
        case RightSide => (_: (Data, Data))._2
      }).right,
      change[T, (Data, Data)])).map(f => (l: Data, r: Data) => f((l, r)))

  def changeReduceFunc[T[_[_]]: Recursive](f: Free[MapFunc[T, ?], ReduceIndex])
      : PlannerError \/ (List[Data] => Data) =
    freeCataM(f)(interpretM(
      ri => ((_: List[Data])(ri.idx)).right,
      change[T, List[Data]]))

  def change[T[_[_]]: Recursive, A]
      : AlgebraM[PlannerError \/ ?, MapFunc[T, ?], A => Data] = {
    case Constant(f) => κ(f.cata(Data.fromEJson)).right
    case Undefined() => κ(undefined).right

    case Length(f) => (f >>> {
      case Data.Str(v) => Data.Int(v.length)
      case Data.Arr(v) => Data.Int(v.size)
      case _ => undefined
    }).right

    case Date(f) => (f >>> {
      case Data.Str(v) => DateLib.parseDate(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Time(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTime(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Timestamp(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTimestamp(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case Interval(f) => (f >>> {
      case Data.Str(v) => DateLib.parseInterval(v).getOrElse(undefined)
      case _ => undefined
    }).right
    case TimeOfDay(f) => (f >>> {
      case Data.Timestamp(v) => Data.Time(v.atZone(ZoneOffset.UTC).toLocalTime)
      case _ => undefined
    }).right
    case ToTimestamp(f) => (f >>> {
      case Data.Int(epoch) => Data.Timestamp(Instant.ofEpochMilli(epoch.toLong))
      case _ => undefined
    }).right
    case ExtractCentury(f) => InternalError("not implemented").left // TODO
    case ExtractDayOfMonth(f) => InternalError("not implemented").left // TODO
    case ExtractDecade(f) => InternalError("not implemented").left // TODO
    case ExtractDayOfWeek(f) => InternalError("not implemented").left // TODO
    case ExtractDayOfYear(f) => InternalError("not implemented").left // TODO
    case ExtractEpoch(f) => InternalError("not implemented").left // TODO
    case ExtractHour(f) => InternalError("not implemented").left // TODO
    case ExtractIsoDayOfWeek(f) => InternalError("not implemented").left // TODO
    case ExtractIsoYear(f) => InternalError("not implemented").left // TODO
    case ExtractMicroseconds(f) => InternalError("not implemented").left // TODO
    case ExtractMillennium(f) => InternalError("not implemented").left // TODO
    case ExtractMilliseconds(f) => InternalError("not implemented").left // TODO
    case ExtractMinute(f) => InternalError("not implemented").left // TODO
    case ExtractMonth(f) => InternalError("not implemented").left // TODO
    case ExtractQuarter(f) => InternalError("not implemented").left // TODO
    case ExtractSecond(f) => InternalError("not implemented").left // TODO
    case ExtractWeek(f) => InternalError("not implemented").left // TODO
    case ExtractYear(f) => InternalError("not implemented").left // TODO
    case Now() => ((x: A) => Data.Timestamp(Instant.now())).right

    case Negate(f) => (f >>> {
      case Data.Int(v) => Data.Int(-v)
      case Data.Dec(v) => Data.Dec(-v)
      case Data.Interval(v) => Data.Interval(v.negated())
      case _ => undefined
    }).right
    case Add(f1, f2) => ((x: A) => add(f1(x), f2(x))).right
    case Multiply(f1, f2) => ((x: A) => multiply(f1(x), f2(x))).right
    case Subtract(f1, f2) => ((x: A) => subtract(f1(x), f2(x))).right
    case Divide(f1, f2) => ((x: A) => divide(f1(x), f2(x))).right
    case Modulo(f1, f2) => ((x: A) => modulo(f1(x), f2(x))).right
    case Power(f1, f2) => ((x: A) => power(f1(x), f2(x))).right

    case Not(f) => (f >>> {
      case Data.Bool(b) => Data.Bool(!b)
      case _ => undefined
    }).right
    case Eq(f1, f2) => ((x: A) => Data.Bool(f1(x) === f2(x))).right
    case Neq(f1, f2) => ((x: A) => Data.Bool(f1(x) =/= f2(x))).right
    case Lt(f1, f2) => ((x: A) => lt(f1(x), f2(x))).right
    case Lte(f1, f2) => ((x: A) => lte(f1(x), f2(x))).right
    case Gt(f1, f2) => ((x: A) => gt(f1(x), f2(x))).right
    case Gte(f1, f2) => ((x: A) => gte(f1(x), f2(x))).right
    case IfUndefined(f1, f2) => ((x: A) => f1(x) match {
      case Data.NA => f2(x)
      case d => d
    }).right
    case And(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a && b)
      case _ => undefined
    }).right
    case Or(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a || b)
      case _ => undefined
    }).right
    case Between(f1, f2, f3) => ((x: A) => between(f1(x), f2(x), f3(x))).right
    case Cond(fCond, fThen, fElse) => ((x: A) => fCond(x) match {
      case Data.Bool(true) => fThen(x)
      case Data.Bool(false) => fElse(x)
      case _ => undefined
    }).right

    case Within(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (d, Data.Arr(list)) => Data.Bool(list.contains(d))
      case _ => undefined
    }).right

    case Lower(f) => (f >>> {
      case Data.Str(a) => Data.Str(a.toLowerCase())
      case _ => undefined
    }).right
    case Upper(f) => (f >>> {
      case Data.Str(a) => Data.Str(a.toUpperCase())
      case _ => undefined
    }).right
    case Bool(f) => (f >>> {
      case Data.Str("true") => Data.Bool(true)
      case Data.Str("false") => Data.Bool(false)
      case _ => undefined
    }).right
    case Integer(f) => (f >>> {
      case Data.Str(a) => \/.fromTryCatchNonFatal(Data.Int(BigInt(a))).fold(κ(Data.NA), ι)
      case _ => undefined
    }).right
    case Decimal(f) => (f >>> {
      case Data.Str(a) => \/.fromTryCatchNonFatal(Data.Dec(BigDecimal(a))).fold(κ(Data.NA), ι)
      case _ => undefined
    }).right
    case Null(f) => (f >>> {
      case Data.Str("null") => Data.Null
      case _ => undefined
    }).right
    case ToString(f) => (f >>> toStringFunc).right
    case Search(fStr, fPattern, fInsen) =>
      ((x: A) => search(fStr(x), fPattern(x), fInsen(x))).right
    case Substring(fStr, fFrom, fCount) =>
      ((x: A) => substring(fStr(x), fFrom(x), fCount(x))).right
    case MakeArray(f) => (f >>> ((x: Data) => Data.Arr(List(x)))).right
    case MakeMap(fK, fV) => ((x: A) => (fK(x), fV(x)) match {
      case (Data.Str(k), v) => Data.Obj(ListMap(k -> v))
      case _ => undefined
    }).right
    case ConcatArrays(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Arr(l1), Data.Arr(l2)) => Data.Arr(l1 ++ l2)
      case (Data.Str(s1), Data.Str(s2)) => Data.Str(s1 ++ s2)
      case _ => undefined
    }).right
    case ConcatMaps(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Obj(m1), Data.Obj(m2)) => Data.Obj(m1 ++ m2)
      case _ => undefined
    }).right
    case ProjectIndex(f1, f2) => ((x: A) => (f1(x), f2(x)) match {
      case (Data.Arr(list), Data.Int(index)) =>
        if(index >= 0 && index < list.size) list(index.toInt) else undefined
      case _ => undefined
    }).right
    case ProjectField(fSrc, fField) => ((x: A) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => m(field)
      case _ => undefined
    }).right
    case DeleteField(fSrc, fField) =>  ((x: A) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => Data.Obj(m - field)
      case _ => undefined
    }).right
    case DupMapKeys(f) => (f >>> {
      case Data.Obj(m) => Data.Obj(ListMap(m.keys.toList.fproduct(Data.Str(_)): _*))
      case _ => undefined
    }).right
    case DupArrayIndices(f) => (f >>> {
      case Data.Arr(l) => Data.Arr(l.indices.map(Data.Int(_)).toList)
      case _ => undefined
    }).right
    case ZipMapKeys(f) => (f >>> {
      case Data.Obj(m) => Data.Obj {
        m.map{
          case (k, v) => (k, Data.Arr(List(Data.Str(k), v)))
        }
      }
      case _ => undefined
    }).right
    case ZipArrayIndices(f) => (f >>> {
      case Data.Arr(l) => Data.Arr(l.zipWithIndex.map {
        case (e, i) => Data.Arr(List(Data.Int(i), e))
      })
      case _ => undefined
    }).right
    case Range(fFrom, fTo) => ((x: A) => (fFrom(x), fTo(x)) match {
      case (Data.Int(a), Data.Int(b)) if(a <= b) => Data.Set((a to b).map(Data.Int(_)).toList)
    }).right
    case Guard(f1, fPattern, f2, ff3) => f2.right
    case _ => InternalError("not implemented").left
  }

  private def add(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) + b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a + BigDecimal(b))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a + b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.plus(b))
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.plus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.plus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.plus(b))
    case _ => undefined
  }

  private def subtract(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a - b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) - b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a - BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a - b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Interval(a.minus(b))
    case (Data.Date(a), Data.Interval(b)) => Data.Date(a.minus(b))
    case (Data.Time(a), Data.Interval(b)) => Data.Time(a.minus(b))
    case (Data.Timestamp(a), Data.Interval(b)) => Data.Timestamp(a.minus(b))
    case _ => undefined
  }

  private def multiply(d1: Data, d2: Data): Data = (d1, d2)  match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a * b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) * b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a * BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Int(a * b)
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => undefined
  }

  private def divide(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(a / b)
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(BigDecimal(a) / b)
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(a / BigDecimal(b))
    case (Data.Int(a), Data.Int(b)) => Data.Dec(BigDecimal(a) / BigDecimal(b))
    case (Data.Interval(a), Data.Dec(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case (Data.Interval(a), Data.Int(b)) => Data.Interval(a.multipliedBy(b.toLong))
    case _ => undefined
  }

  // TODO other cases?
  private def modulo(d1: Data, d2: Data) = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Int(a % b)
    case (Data.Int(a), Data.Dec(b)) => ???
    case (Data.Dec(a), Data.Int(b)) => ???
    case (Data.Dec(a), Data.Dec(b)) => ???
    case (Data.Interval(a), Data.Int(b)) => ???
    case (Data.Interval(a), Data.Dec(b)) => ???
    case _ => undefined
  }

  // TODO we loose precision here, consider using https://github.com/non/spire/
  private def power(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Int(a), Data.Dec(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Dec(a), Data.Int(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case (Data.Dec(a), Data.Dec(b)) => Data.Dec(math.pow(a.toDouble, b.toDouble))
    case _ => undefined
  }

  private def lt(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a < b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a < b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) < 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (false, true) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  private def lte(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a <= b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a <= b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) <= 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (true, false) => Data.Bool(false)
      case _ => Data.Bool(true)
    }
    case _ => undefined
  }

  private def gt(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a > b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a > b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) > 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (true, false) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  private def gte(d1: Data, d2: Data): Data = (d1, d2) match {
    case (Data.Int(a), Data.Int(b)) => Data.Bool(a >= b)
    case (Data.Dec(a), Data.Dec(b)) => Data.Bool(a >= b)
    case (Data.Interval(a), Data.Interval(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Str(a), Data.Str(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Timestamp(a), Data.Timestamp(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Date(a), Data.Date(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Time(a), Data.Time(b)) => Data.Bool(a.compareTo(b) >= 0)
    case (Data.Bool(a), Data.Bool(b)) => (a, b) match {
      case (false, true) => Data.Bool(false)
      case _ => Data.Bool(true)
    }
    case _ => undefined
  }

  private def between(d1: Data, d2: Data, d3: Data): Data = (d1, d2, d3) match {
    case (Data.Int(a), Data.Int(b), Data.Int(c)) =>
      Data.Bool(b <= a && a <= c)
    case (Data.Dec(a), Data.Dec(b), Data.Dec(c)) =>
      Data.Bool(b <= a && a <= c)
    case (Data.Interval(a), Data.Interval(b), Data.Interval(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Str(a), Data.Str(b), Data.Str(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Timestamp(a), Data.Timestamp(b), Data.Timestamp(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Date(a), Data.Date(b), Data.Date(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Time(a), Data.Time(b), Data.Time(c)) =>
      Data.Bool(b.compareTo(a) <= 0 && a.compareTo(c) <= 0)
    case (Data.Bool(a), Data.Bool(b), Data.Bool(c)) => (a,b,c) match {
      case (false, false, true) => Data.Bool(true)
      case (false, true, true) => Data.Bool(true)
      case (true, true, true) => Data.Bool(true)
      case _ => Data.Bool(false)
    }
    case _ => undefined
  }

  // TODO reuse render from codec
  private def toStringFunc: Data => Data = {
    case Data.Null => Data.Str("null")
    case d: Data.Str => d
    case Data.Bool(v) => Data.Str(v.toString)
    case Data.Dec(v) => Data.Str(v.toString)
    case Data.Int(v) => Data.Str(v.toString)
    case Data.Timestamp(v) => Data.Str(v.toString)
    case Data.Date(v) => Data.Str(v.toString)
    case Data.Time(v) => Data.Str(v.toString)
    case Data.Interval(v) => Data.Str(v.toString)
    case Data.Binary(v) => Data.Str(v.toList.mkString(""))
    case Data.Id(s) => Data.Str(s)
    case _ => undefined
  }

  private def search(dStr: Data, dPattern: Data, dInsen: Data): Data =
    (dStr, dPattern, dInsen) match {
      case (Data.Str(str), Data.Str(pattern), Data.Bool(insen)) =>
        Data.Bool(StringLib.matchAnywhere(str, pattern, insen))
      case _ => undefined
    }

  private def substring(dStr: Data, dFrom: Data, dCount: Data): Data =
    (dStr, dFrom, dCount) match {
      case (Data.Str(str), Data.Int(from), Data.Int(count)) =>
        \/.fromTryCatchNonFatal(Data.Str(StringLib.safeSubstring(str, from.toInt, count.toInt))).fold(κ(Data.NA), ι)
      case _ => undefined
    }

}
