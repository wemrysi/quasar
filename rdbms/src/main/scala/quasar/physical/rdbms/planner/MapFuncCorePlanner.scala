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
import quasar.Data
import quasar.Planner._
import quasar.contrib.scalaz.MonadError_
import quasar.physical.rdbms.planner.sql.{SqlExpr => SQL}
import quasar.qscript.{MapFuncsCore => MFC, _}

import matryoshka._
import matryoshka.implicits._
import scalaz.{Divide => _,Split => _, _}, Scalaz._


class MapFuncCorePlanner[T[_[_]]: BirecursiveT: ShowT, F[_]:Applicative:PlannerErrorME]
    extends Planner[T, F, MapFuncCore[T, ?]] {

  val undefined: T[SQL] = SQL.Null[T[SQL]]().embed

  private def notImplemented: F[T[SQL]] =
    MonadError_[F, PlannerError].raiseError(InternalError.fromMsg("not implemented"))

  def plan: AlgebraM[F, MapFuncCore[T, ?], T[SQL]] = {
    case MFC.Constant(ejson) => SQL.Constant[T[SQL]](ejson.cata(Data.fromEJson)).embed.η[F]
    case MFC.Undefined() =>  undefined.η[F]
    case MFC.JoinSideName(n) =>  notImplemented
    case MFC.Length(f) => notImplemented
    case MFC.Date(f) =>  notImplemented
    case MFC.Time(f) =>  notImplemented
    case MFC.Timestamp(f) =>  notImplemented
    case MFC.Interval(f) =>  notImplemented
    case MFC.StartOfDay(f) =>  notImplemented
    case MFC.TemporalTrunc(p, f) =>  notImplemented
    case MFC.TimeOfDay(f) =>  notImplemented
    case MFC.ToTimestamp(f) =>  notImplemented
    case MFC.ExtractCentury(f) =>  notImplemented
    case MFC.ExtractDayOfMonth(f) =>  notImplemented
    case MFC.ExtractDecade(f) =>  notImplemented
    case MFC.ExtractDayOfWeek(f) =>  notImplemented
    case MFC.ExtractDayOfYear(f) =>  notImplemented
    case MFC.ExtractEpoch(f) =>  notImplemented
    case MFC.ExtractHour(f) =>  notImplemented
    case MFC.ExtractIsoDayOfWeek(f) =>  notImplemented
    case MFC.ExtractIsoYear(f) =>  notImplemented
    case MFC.ExtractMicroseconds(f) => notImplemented
    case MFC.ExtractMillennium(f) =>  notImplemented
    case MFC.ExtractMilliseconds(f) =>  notImplemented
    case MFC.ExtractMinute(f) =>  notImplemented
    case MFC.ExtractMonth(f) =>  notImplemented
    case MFC.ExtractQuarter(f) =>  notImplemented
    case MFC.ExtractSecond(f) =>  notImplemented
    case MFC.ExtractWeek(f) =>  notImplemented
    case MFC.ExtractYear(f) =>  notImplemented
    case MFC.Now() =>  notImplemented
    case MFC.Negate(f) =>  notImplemented
    case MFC.Add(f1, f2) =>  SQL.NumericOp[T[SQL]]("+", f1, f2).embed.η[F]
    case MFC.Multiply(f1, f2) => SQL.NumericOp[T[SQL]]("*", f1, f2).embed.η[F]
    case MFC.Subtract(f1, f2) =>  SQL.NumericOp[T[SQL]]("-", f1, f2).embed.η[F]
    case MFC.Divide(f1, f2) => SQL.NumericOp[T[SQL]]("/", f1, f2).embed.η[F]
    case MFC.Modulo(f1, f2) => notImplemented
    case MFC.Power(f1, f2) =>  notImplemented
    case MFC.Not(f) =>  notImplemented
    case MFC.Eq(f1, f2) =>  notImplemented
    case MFC.Neq(f1, f2) =>  notImplemented
    case MFC.Lt(f1, f2) =>  notImplemented
    case MFC.Lte(f1, f2) => notImplemented
    case MFC.Gt(f1, f2) =>  notImplemented
    case MFC.Gte(f1, f2) => notImplemented
    case MFC.IfUndefined(f1, f2) => notImplemented
    case MFC.And(f1, f2) =>  notImplemented
    case MFC.Or(f1, f2) =>  notImplemented
    case MFC.Between(f1, f2, f3) =>  notImplemented
    case MFC.Cond(fCond, fThen, fElse) =>  notImplemented
    case MFC.Within(f1, f2) =>  notImplemented
    case MFC.Lower(f) =>  notImplemented
    case MFC.Upper(f) =>  notImplemented
    case MFC.Bool(f) =>  notImplemented
    case MFC.Integer(f) =>  notImplemented
    case MFC.Decimal(f) =>  notImplemented
    case MFC.Null(f) =>  notImplemented
    case MFC.ToString(f) =>  notImplemented
    case MFC.Search(fStr, fPattern, fInsen) => notImplemented
    case MFC.Substring(fStr, fFrom, fCount) => notImplemented
    case MFC.Split(fStr, fDelim) => notImplemented
    case MFC.MakeArray(f) =>  notImplemented
    case MFC.MakeMap(fK, fV) =>  notImplemented
    case MFC.ConcatArrays(f1, f2) =>  notImplemented
    case MFC.ConcatMaps(f1, f2) =>  notImplemented
    case MFC.ProjectIndex(f1, f2) =>  notImplemented
    case MFC.ProjectKey(fSrc, fKey) => SQL.Ref[T[SQL]](fSrc, fKey).embed.η[F]
    case MFC.DeleteKey(fSrc, fField) =>   notImplemented
    case MFC.Range(fFrom, fTo) =>  notImplemented
    case MFC.Guard(f1, fPattern, f2, ff3) => f2.η[F]
    case MFC.TypeOf(f) => notImplemented
    case _ => notImplemented
  }
}
