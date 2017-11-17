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
import quasar.{NameGenerator, qscript}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.physical.rdbms.planner.sql.SqlExpr._
import quasar.physical.rdbms.planner.sql.{SqlExpr, genId}
import quasar.qscript.{FreeMap, MapFunc, QScriptCore}

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.Scalaz._
import scalaz._

class QScriptCorePlanner[T[_[_]]: CorecursiveT,
F[_]: Monad: NameGenerator: PlannerErrorME](
    mapFuncPlanner: Planner[T, F, MapFunc[T, ?]])
    extends Planner[T, F, QScriptCore[T, ?]] {

  def processFreeMap(f: FreeMap[T], alias: SqlExpr.Id[T[SqlExpr]]): F[T[SqlExpr]] =
    f.cataM(interpretM(κ(alias.embed.η[F]), mapFuncPlanner.plan))

  def plan: AlgebraM[F, QScriptCore[T, ?], T[SqlExpr]] = {
    case qscript.Map(src, f) =>
      for {
        generatedAlias <- genId[T[SqlExpr], F]
        selection <- processFreeMap(f, generatedAlias)
      } yield
        Select(
          Selection(selection, none),
          From(src, generatedAlias.some),
          filter = none
        ).embed

    case other =>
      PlannerErrorME[F].raiseError(
        InternalError.fromMsg(s"unsupported QScriptCore: $other"))
  }
}
