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

import slamdata.Predef._
import quasar.fp.ski._
import quasar.{NameGenerator}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.sql.{SqlExpr}, SqlExpr._
import quasar.qscript.{MapFunc, JoinFunc, EquiJoin, QScriptTotal, LeftSide, RightSide}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

class EquiJoinPlanner[T[_[_]]: BirecursiveT: ShowT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, EquiJoin[T, ?]] {

  private def processJoinFunc(
    f: JoinFunc[T],
    leftAlias: SqlExpr[T[SqlExpr]],
    rightAlias: SqlExpr[T[SqlExpr]]
  ): F[T[SqlExpr]] =
    f.cataM(interpretM({
      case LeftSide  => leftAlias.embed.η[F]
      case RightSide => rightAlias.embed.η[F]
    }, mapFuncPlanner.plan))

  private def unsupported: F[T[SqlExpr]] = PlannerErrorME[F].raiseError(
        InternalError.fromMsg(s"unsupported EquiJoin"))

  val unref: T[SqlExpr] = SqlExpr.Unreferenced[T[SqlExpr]]().embed

  def plan: AlgebraM[F, EquiJoin[T, ?], T[SqlExpr]] = {
    case EquiJoin(src, lBranch, rBranch, key, joinType, combine) =>
      val compile = Planner[T, F, QScriptTotal[T, ?]].plan
      val left: F[T[SqlExpr]] = lBranch.cataM(interpretM(κ(src.point[F]), compile))
      val right: F[T[SqlExpr]] = rBranch.cataM(interpretM(κ(src.point[F]), compile))

      unsupported
  }
}
