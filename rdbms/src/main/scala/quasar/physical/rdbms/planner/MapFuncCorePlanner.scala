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

package quasar.physical.rdbms.planner

import slamdata.Predef.{Eq => _}
import quasar.Planner._
import quasar.contrib.scalaz.MonadError_
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.qscript._, MapFuncsCore._

import matryoshka._
import scalaz.{Divide => _,Split => _}


class MapFuncCorePlanner[T[_[_]], F[_]:PlannerErrorME] extends Planner[T, F, MapFuncCore[T, ?]] {

  private def notImplemented: F[T[SqlExpr]] =
    MonadError_[F, PlannerError].raiseError(InternalError.fromMsg("not implemented"))

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[SqlExpr]] = {
    case Constant(f) =>  notImplemented
    case Undefined() =>  notImplemented
    case JoinSideName(n) =>  notImplemented
    case Length(f) => notImplemented
    case Date(f) =>  notImplemented
    case Time(f) =>  notImplemented
    case Timestamp(f) =>  notImplemented
    case Interval(f) =>  notImplemented
    case StartOfDay(f) =>  notImplemented
    case TemporalTrunc(p, f) =>  notImplemented
    case TimeOfDay(f) =>  notImplemented
    case ToTimestamp(f) =>  notImplemented
    case ExtractCentury(f) =>  notImplemented
    case ExtractDayOfMonth(f) =>  notImplemented
    case ExtractDecade(f) =>  notImplemented
    case ExtractDayOfWeek(f) =>  notImplemented
    case ExtractDayOfYear(f) =>  notImplemented
    case ExtractEpoch(f) =>  notImplemented
    case ExtractHour(f) =>  notImplemented
    case ExtractIsoDayOfWeek(f) =>  notImplemented
    case ExtractIsoYear(f) =>  notImplemented
    case ExtractMicroseconds(f) => notImplemented
    case ExtractMillennium(f) =>  notImplemented
    case ExtractMilliseconds(f) =>  notImplemented
    case ExtractMinute(f) =>  notImplemented
    case ExtractMonth(f) =>  notImplemented
    case ExtractQuarter(f) =>  notImplemented
    case ExtractSecond(f) =>  notImplemented
    case ExtractWeek(f) =>  notImplemented
    case ExtractYear(f) =>  notImplemented
    case Now() =>  notImplemented
    case Negate(f) =>  notImplemented
    case Add(f1, f2) =>  notImplemented
    case Multiply(f1, f2) => notImplemented
    case Subtract(f1, f2) =>  notImplemented
    case Divide(f1, f2) => notImplemented
    case Modulo(f1, f2) => notImplemented
    case Power(f1, f2) =>  notImplemented
    case Not(f) =>  notImplemented
    case Eq(f1, f2) =>  notImplemented
    case Neq(f1, f2) =>  notImplemented
    case Lt(f1, f2) =>  notImplemented
    case Lte(f1, f2) => notImplemented
    case Gt(f1, f2) =>  notImplemented
    case Gte(f1, f2) => notImplemented
    case IfUndefined(f1, f2) => notImplemented
    case And(f1, f2) =>  notImplemented
    case Or(f1, f2) =>  notImplemented
    case Between(f1, f2, f3) =>  notImplemented
    case Cond(fCond, fThen, fElse) =>  notImplemented
    case Within(f1, f2) =>  notImplemented
    case Lower(f) =>  notImplemented
    case Upper(f) =>  notImplemented
    case Bool(f) =>  notImplemented
    case Integer(f) =>  notImplemented
    case Decimal(f) =>  notImplemented
    case Null(f) =>  notImplemented
    case ToString(f) =>  notImplemented
    case Search(fStr, fPattern, fInsen) => notImplemented
    case Substring(fStr, fFrom, fCount) => notImplemented
    case Split(fStr, fDelim) => notImplemented
    case MakeArray(f) =>  notImplemented
    case MakeMap(fK, fV) =>  notImplemented
    case ConcatArrays(f1, f2) =>  notImplemented
    case ConcatMaps(f1, f2) =>  notImplemented
    case ProjectIndex(f1, f2) =>  notImplemented
    case ProjectKey(fSrc, fKey) =>  notImplemented
    case DeleteKey(fSrc, fField) =>   notImplemented
    case Range(fFrom, fTo) =>  notImplemented
    case Guard(f1, fPattern, f2, ff3) => notImplemented
    case TypeOf(f) => notImplemented
    case _ => notImplemented
  }
}
