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
import quasar.common.SortDir.{Ascending, Descending}
import quasar.Data
import quasar.DataCodec
import quasar.DataCodec.Precise.TimeKey
import quasar.physical.rdbms.model._
import quasar.physical.rdbms.fs.postgres._
import quasar.physical.rdbms.planner.RenderQuery
import quasar.physical.rdbms.planner.sql._
import quasar.physical.rdbms.planner.sql.SqlExpr.Select._
import quasar.physical.rdbms.planner.sql.SqlExpr.Case._
import quasar.Planner.InternalError
import quasar.Planner.{NonRepresentableData, PlannerError}

import matryoshka._
import matryoshka.implicits._

import scalaz._
import Scalaz._

object PostgresRenderQuery extends RenderQuery {
  import SqlExpr._

  implicit val codec: DataCodec = DataCodec.Precise

  def asString[T[_[_]]: BirecursiveT](a: T[SqlExpr]): PlannerError \/ String = {
    val q = a.transCataT(RelationalOperations.processQuotes).paraM(galg)
    q ∘ (s => s"select row_to_json(row) from ($s) as row")
  }

  def alias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" as ${i.v}"))

  def rowAlias(a: Option[SqlExpr.Id[String]]) = ~(a ∘ (i => s" ${i.v}"))

  def buildJson(str: String): String =
    s"json_build_object($str)#>>'{}'"

  sealed trait JsonRefType {
    def arrow: String
  }

  case object TextRef extends JsonRefType {
    override def arrow: String = "->>"
  }

  case object JsonRef extends JsonRefType {
    override def arrow: String = "->"
  }

  def renderRefs[T[_[_]]: BirecursiveT](refs: Refs[(T[SqlExpr], String)], refType: JsonRefType)
  : PlannerError \/ String = {
    refs.elems match {
      case Vector((_, key), (_, value)) =>
        val valueStripped = value.stripPrefix("'").stripSuffix("'")
        s"""$key.$valueStripped""".right
      case key +: mid :+ last =>
        val firstValStripped = ~mid.headOption.map(_._2.stripPrefix("'").stripSuffix("'"))
        val midTail = mid.drop(1)
        val midStr = if (midTail.nonEmpty)
          s"->${midTail.map(e => s"$e").intercalate("->")}"
        else
          ""
        s"""${key._2}.$firstValStripped$midStr${refType.arrow}${last._2}""".right
      case _ => InternalError.fromMsg(s"Cannot process $refs").left
    }
  }

  def galg[T[_[_]]: BirecursiveT]: GAlgebraM[(T[SqlExpr], ?), PlannerError \/ ?, SqlExpr, String] = {
    case Unreferenced() =>
    InternalError("Unexpected Unreferenced!", none).left
    case Null() => "null".right
    case SqlExpr.Id(v) =>
      s"""$v""".right
    case Table(v) =>
      v.right
    case AllCols() =>
      s"*".right
    case r@Refs(srcs) =>
      renderRefs(r, JsonRef)
    case Obj(m) =>
      buildJson(m.map {
        case ((_, k), (_, v)) => s"'$k', $v"
      }.mkString(",")).right
    case RegexMatches((_, str), (_, pattern)) =>
      s"($str ~ '$pattern')".right
    case IsNotNull(expr) =>
      s"($expr notnull)".right
    case IfNull(a) =>
      s"coalesce(${a.map(_._2).intercalate(", ")})".right
    case ExprWithAlias((_, expr), alias) =>
      (if (expr === alias) s"$expr" else {
        val aliasStr = \/.fromTryCatchNonFatal(alias.toLong).map(a => s""""$a"""").getOrElse(alias)
        s"$expr as $aliasStr"
      }).right
    case ExprPair((_, expr1), (_, expr2)) =>
      s"$expr1, $expr2".right
    case ConcatStr((_, str1), (_, str2))  =>
      s"$str1 || $str2".right
    case Time((_, expr)) =>
      buildJson(s"""{ "$TimeKey": $expr }""").right
    case NumericOp(sym, (_, left), (_, right)) =>
      s"(($left)::text::numeric $sym ($right)::text::numeric)".right
    case Mod((_, a1), (_, a2)) =>
      s"mod(($a1)::text::numeric, ($a2)::text::numeric)".right
    case Pow((_, a1), (_, a2)) =>
      s"power(($a1)::text::numeric, ($a2)::text::numeric)".right
    case And((_, a1), (_, a2)) =>
      s"($a1 and $a2)".right
    case Or((_, a1), (_, a2)) =>
      s"($a1 or $a2)".right
    case Neg((_, str)) =>
      s"(-$str)".right
    case Eq((_, a1), (_, a2)) =>
      s"(($a1)::text = ($a2)::text)".right
    case Neq((_, a1), (_, a2)) =>
      s"(($a1)::text != ($a2)::text)".right
    case Lt((_, a1), (_, a2)) =>
      s"(($a1)::text::numeric < ($a2)::text::numeric)".right
    case Lte((_, a1), (_, a2)) =>
      s"(($a1)::text::numeric <= ($a2)::text::numeric)".right
    case Gt((_, a1), (_, a2)) =>
      s"(($a1)::text::numeric > ($a2)::text::numeric)".right
    case Gte((_, a1), (_, a2)) =>
      s"(($a1)::text::numeric >= ($a2)::text::numeric)".right
    case WithIds((_, str))    => s"(row_number() over(), $str)".right
    case RowIds()        => "row_number() over()".right
    case Offset((_, from), (_, count)) => s"$from OFFSET $count".right
    case Limit((_, from), (_, count)) => s"$from LIMIT $count".right
    case Select(selection, from, filterOpt, order) =>
      val filter = ~(filterOpt ∘ (f => s" where ${f.v}"))
      val orderStr = order.map { o =>
        val dirStr = o.sortDir match {
          case Ascending => "asc"
          case Descending => "desc"
        }
        s"${o.v} $dirStr"
      }.mkString(", ")

      val orderByStr = if (order.nonEmpty)
        s" order by $orderStr"
      else
        ""

      val fromExpr = s" from ${from.v} ${from.alias.v}"
      s"(select ${selection.v}$fromExpr$filter$orderByStr)".right
    case Constant(Data.Str(v)) =>
      val text = v.flatMap { case ''' => "''"; case iv => iv.toString }.self
      s"'$text'".right
    case Constant(v) =>
      DataCodec.render(v) \/> NonRepresentableData(v)
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen((_, w), (_, t)) => s"when $w then $t" }
      s"(case ${wts.intercalate(" ")} else ${e.v} end)".right
    case Coercion(t, e) => s"($e)::${t.mapToStringName}".right
    case UnaryFunction(fType, (_, e)) =>
      val fName = fType match {
        case StrLower => "lower"
        case StrUpper => "upper"
      }
      s"$fName($e)".right
    case BinaryFunction(fType, (_, a1), (_, a2)) =>
      val fName = fType match {
        case SplitStr => "regexp_split_to_array"
      }
      s"$fName($a1, $a2)".right
    case TernaryFunction(fType, (_, a1), (_, a2), (_, a3)) => (fType match {
      case Search => s"(case when $a3 then $a1 ~* $a2 else $a1 ~ $a2 end)"
      case Substring => s"substring($a1 from ($a2+1) for $a3)"
    }).right
  }
}
