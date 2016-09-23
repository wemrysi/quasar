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

import quasar.Predef._
import quasar.qscript.MapFuncs._
import quasar.std.{DateLib, StringLib}
import quasar.Data
import quasar.qscript._
import quasar.Planner._
import quasar.SKI._

import scala.math

import org.threeten.bp.{Instant, ZoneOffset}
import matryoshka.{Hole => _, _}, Recursive.ops._
import scalaz.{Divide => _, _}, Scalaz._

object CoreMap {

  private val undefined = Data.NA

  // TODO: replace Data.NA with something safer
  def change[T[_[_]] : Recursive]: AlgebraM[PlannerError \/ ?, MapFunc[T, ?], Data => Data] = {
    case Constant(f) => κ[Data, Data](f.cata(Data.fromEJson)).right
    case Undefined() => κ[Data, Data](Data.NA).right // TODO compback to this one, needs reviewv

    case Length(f) => (f >>> {
      case Data.Str(v) => Data.Int(v.length)
      case Data.Arr(v) => Data.Int(v.size)
      case _ => undefined 
    }).right

    case Date(f) => (f >>> {
      case Data.Str(v) => DateLib.parseDate(v).getOrElse(Data.NA)
      case _ => undefined
    }).right
    case Time(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTime(v).getOrElse(Data.NA)
      case _ => undefined
    }).right
    case Timestamp(f) => (f >>> {
      case Data.Str(v) => DateLib.parseTimestamp(v).getOrElse(Data.NA)
      case _ => undefined
    }).right
    case Interval(f) => (f >>> {
      case Data.Str(v) => DateLib.parseInterval(v).getOrElse(Data.NA)
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
    case Extract(f1, f2) => InternalError("not implemented").left // TODO - waits for Moss changes
    case Now() => ((x: Data) => Data.Timestamp(Instant.now())).right

    case Negate(f) => (f >>> {
      case Data.Int(v) => Data.Int(-v)
      case Data.Dec(v) => Data.Dec(-v)
      case Data.Interval(v) => Data.Interval(v.negated())
      case _ => undefined
    }).right
    case Add(f1, f2) => ((x: Data) => add(f1(x), f2(x))).right
    case Multiply(f1, f2) => ((x: Data) => multiply(f1(x), f2(x))).right
    case Subtract(f1, f2) => ((x: Data) => subtract(f1(x), f2(x))).right
    case Divide(f1, f2) => ((x: Data) => divide(f1(x), f2(x))).right
    case Modulo(f1, f2) => ((x: Data) => modulo(f1(x), f2(x))).right
    case Power(f1, f2) => ((x: Data) => power(f1(x), f2(x))).right

    case Not(f) => (f >>> {
      case Data.Bool(b) => Data.Bool(!b)
      case _ => undefined
    }).right
    case Eq(f1, f2) => ((x: Data) => Data.Bool(f1(x) === f2(x))).right
    case Neq(f1, f2) => ((x: Data) => Data.Bool(f1(x) =/= f2(x))).right
    case Lt(f1, f2) => ((x: Data) => lt(f1(x), f2(x))).right
    case Lte(f1, f2) => ((x: Data) => lte(f1(x), f2(x))).right
    case Gt(f1, f2) => ((x: Data) => gt(f1(x), f2(x))).right
    case Gte(f1, f2) => ((x: Data) => gte(f1(x), f2(x))).right
    case IfUndefined(f1, f2) => ((x: Data) => f1(x) match {
      case Data.NA => f2(x)
      case d => d
    }).right
    case And(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a && b)
      case _ => undefined
    }).right
    case Or(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Bool(a), Data.Bool(b)) => Data.Bool(a || b)
      case _ => undefined
    }).right
    case Coalesce(f1, f2) => ((x: Data) => f1(x) match {
      case Data.Null => f2(x)
      case d => d
    }).right
    case Between(f1, f2, f3) => ((x: Data) => between(f1(x), f2(x), f3(x))).right
    case Cond(fCond, fThen, fElse) => ((x: Data) => fCond(x) match {
      case Data.Bool(true) => fThen(x)
      case Data.Bool(false) => fElse(x)
      case _ => undefined
    }).right
      
    case Within(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
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
      ((x: Data) => search(fStr(x), fPattern(x), fInsen(x))).right
    case Substring(fStr, fFrom, fCount) =>
      ((x: Data) => substring(fStr(x), fFrom(x), fCount(x))).right
    case MakeArray(f) => (f >>> ((x: Data) => Data.Arr(List(x)))).right
    case MakeMap(fK, fV) => ((x: Data) => (fK(x), fV(x)) match {
      case (Data.Str(k), v) => Data.Obj(ListMap(k -> v))
      case _ => undefined
    }).right
    case ConcatArrays(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Arr(l1), Data.Arr(l2)) => Data.Arr(l1 ++ l2)
      case _ => undefined
    }).right
    case ConcatMaps(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Obj(m1), Data.Obj(m2)) => Data.Obj{
        m1.foldLeft(m2){
          case (acc, (k, v)) => if(acc.isDefinedAt(k)) acc else acc + (k -> v)
        }
      }
      case _ => undefined
    }).right
    case ProjectIndex(f1, f2) => ((x: Data) => (f1(x), f2(x)) match {
      case (Data.Arr(list), Data.Int(index)) =>
        if(index >= 0 && index < list.size) list(index.toInt) else undefined
      case _ => undefined
    }).right
    case ProjectField(fSrc, fField) => ((x: Data) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => m(field)
      case _ => undefined
    }).right
    case DeleteField(fSrc, fField) =>  ((x: Data) => (fSrc(x), fField(x)) match {
      case (Data.Obj(m), Data.Str(field)) if m.isDefinedAt(field) => Data.Obj(m - field)
      case _ => undefined
    }).right
    case DupMapKeys(f) => InternalError("DupMapKeys not implemented").left
    case DupArrayIndices(f) => InternalError("DupArrayIndices not implemented").left
    case ZipMapKeys(f) => InternalError("ZipMapKeys not implemented").left
    case ZipArrayIndices(f) => InternalError("ZipArrayIndices not implemented").left
    case Range(fFrom, fTo) => InternalError("Range not implemented").left
    case Guard(f1, fPattern, f2,ff3) => InternalError("Guard not implemented").left
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
    case (Data.Int(a), Data.Int(b)) => Data.Int(a / b)
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
      Data.Bool(a <= b && b <= c)
    case (Data.Dec(a), Data.Dec(b), Data.Dec(c)) =>
      Data.Bool(a <= b && b <= c)
    case (Data.Interval(a), Data.Interval(b), Data.Interval(c)) =>
      Data.Bool(a.compareTo(b) <= 0 && b.compareTo(c) <= 0)
    case (Data.Str(a), Data.Str(b), Data.Str(c)) =>
      Data.Bool(a.compareTo(b) <= 0 && b.compareTo(c) <= 0)
    case (Data.Timestamp(a), Data.Timestamp(b), Data.Timestamp(c)) =>
      Data.Bool(a.compareTo(b) <= 0 && b.compareTo(c) <= 0)
    case (Data.Date(a), Data.Date(b), Data.Date(c)) =>
      Data.Bool(a.compareTo(b) <= 0 && b.compareTo(c) <= 0)
    case (Data.Time(a), Data.Time(b), Data.Time(c)) =>
      Data.Bool(a.compareTo(b) <= 0 && b.compareTo(c) <= 0)
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
    // TODO how to handle obj and collections
    case Data.Obj(v) => ???
    case Data.Arr(v) => ???
    case Data.Set(v) => ???
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
        \/.fromTryCatchNonFatal(Data.Str(str.substring(from.toInt, count.toInt))).fold(κ(Data.NA), ι)
      case _ => undefined
    }

}
