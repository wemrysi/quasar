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

package quasar.physical.rdbms.fs.postgres.planner

import slamdata.Predef._
import quasar.Data
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql.SqlExpr
import quasar.DataCodec

import matryoshka._
import matryoshka.implicits._
import scalaz.Scalaz._
import scalaz._

object PostgresRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {
    val q = a.cataM(alg)

    a.project match {
      case s: Select[T[SqlExpr]] => q ∘ (s => s"$s")
      case _                     => q ∘ ("" ⊹ _)
    }
  }
  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  val alg: AlgebraM[PlannerError \/ ?, SqlExpr, String] = {
    case Null() => "null".right
    case SqlExpr.Id(v) =>
      s"""$v""".right
    case Table(v) =>
      v.right
    case AllCols(alias) =>
      s"row_to_json($alias)".right
    case Ref(src, ref) => s"$src->>'$ref'".right
    case Op(sym, left, right) => s"(($left)::numeric $sym ($right)::numeric)".right
    case WithIds(str)    => s"(row_number() over(), $str)".right
    case RowIds()        => "row_number() over()".right
    case Select(selection, from, filterOpt) =>
      val selectionStr = selection.v ⊹ alias(selection.alias)
      val filter = ~(filterOpt ∘ (f => s" where ${f.v}"))
      val fromExpr = s" from ${from.v}" ⊹ alias(from.alias)
      s"(select $selectionStr$fromExpr$filter)".right
    case SelectRow(selection, from) =>
      val fromExpr = s" from ${from.v}"
      s"(select ${selection.v}${rowAlias(selection.alias)}$fromExpr${rowAlias(selection.alias)})".right
    case Constant(Data.Str(v)) =>
      v.flatMap { case ''' => "''"; case iv => iv.toString }.self.right
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
  }
}
