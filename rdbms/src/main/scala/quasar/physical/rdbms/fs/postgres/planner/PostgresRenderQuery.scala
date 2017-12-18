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
    a.paraM(galg) ∘ (s => s"select row_to_json(row) from ($s) as row")
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

  def renderRefs[T[_[_]]: BirecursiveT](elems: Vector[String], refType: JsonRefType)
  : PlannerError \/ String = {
    elems match {
      case Vector(key, value) =>
        val valueStripped = value.stripPrefix("'").stripSuffix("'")
        s"""$key.$valueStripped""".right
      case key +: mid :+ last =>
        val firstValStripped = ~mid.headOption.map(_.stripPrefix("'").stripSuffix("'"))
        val midTail = mid.drop(1)
        val midStr = if (midTail.nonEmpty)
          s"->${midTail.map(e => s"$e").intercalate("->")}"
        else
          ""
        s"""$key.$firstValStripped$midStr${refType.arrow}$last""".right
      case _ => InternalError.fromMsg(s"Cannot process refs: $elems").left
    }
  }

  def text[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = {
    val (expr, str) = pair
    val toReplace = "->"
    val replacement = "->>"
    expr.project match {
      case Refs(_) =>
        val pos = str.lastIndexOf(toReplace)
        if (pos > 0 && !str.contains(replacement))
          str.substring(0, pos) + replacement + str.substring(pos + toReplace.length, replacement.length)
        else
          str
      case _ =>
        str
    }
  }

  def num[T[_[_]]: BirecursiveT](pair: (T[SqlExpr], String)): String = s"(${text(pair)})::numeric"

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
    case Refs(elems) =>
      renderRefs(elems.map(_._2), JsonRef)
    case Obj(m) =>
      buildJson(m.map {
        case ((_, k), (_, v)) => s"'$k', $v"
      }.mkString(",")).right
    case RegexMatches(expr, (_, pattern)) =>
      s"(${text(expr)} ~ '$pattern')".right
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
    case ConcatStr(e1, e2)  =>
      s"${text(e1)} || ${text(e2)}".right
    case Time((_, expr)) =>
      buildJson(s"""{ "$TimeKey": $expr }""").right
    case NumericOp(sym, left, right) =>
      s"(${num(left)} $sym ${num(right)})".right
    case Mod(a1, a2) =>
      s"mod(${num(a1)}, ${num(a2)})".right
    case Pow(a1, a2) =>
      s"power(${num(a1)}, ${num(a2)})".right
    case And((_, a1), (_, a2)) =>
      s"($a1 and $a2)".right
    case Or((_, a1), (_, a2)) =>
      s"($a1 or $a2)".right
    case Neg(e) =>
      s"(-${num(e)})".right
    case Eq(a1, a2) =>
      s"(${text(a1)} = ${text(a2)})".right
    case Neq(a1, a2) =>
      s"(${text(a1)} != ${text(a2)})".right
    case Lt(a1, a2) =>
      s"(${num(a1)} < ${num(a2)})".right
    case Lte(a1, a2) =>
      s"(${num(a1)} <= ${num(a2)})".right
    case Gt(a1, a2) =>
      s"(${num(a1)} > ${num(a2)})".right
    case Gte(a1, a2) =>
      s"(${num(a1)} >= ${num(a2)})".right
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
