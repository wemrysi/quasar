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

package quasar.physical.marklogic.xquery

import quasar.Predef._

import java.lang.SuppressWarnings

import scalaz._
import scalaz.std.string._
import scalaz.std.iterable._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

@SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
object expr {
  def attribute(name: XQuery)(content: XQuery): XQuery =
    XQuery(s"attribute {$name} {$content}")

  def element(name: XQuery)(content: XQuery): XQuery =
    XQuery(s"element {$name} {$content}")

  val emptySeq: XQuery =
    XQuery("()")

  def every(ts: (String, XQuery), tss: (String, XQuery)*): QuantifiedExpr =
    QuantifiedExpr(Quantifier.Every, NonEmptyList(ts, tss: _*))

  def for_(ts: (String, XQuery), tss: (String, XQuery)*): Flwor =
    Flwor(ts :: IList.fromList(tss.toList), IList.empty, None, IList.empty, false)

  def func(args: String*)(body: XQuery): XQuery =
    XQuery(s"function${mkSeq(args map (XQuery(_)))} { $body }")

  def if_(cond: XQuery): IfExpr =
    IfExpr(cond)

  def let_(b: (String, XQuery), bs: (String, XQuery)*): Flwor =
    Flwor(IList.empty, b :: IList.fromList(bs.toList), None, IList.empty, false)

  def some(ts: (String, XQuery), tss: (String, XQuery)*): QuantifiedExpr =
    QuantifiedExpr(Quantifier.Some, NonEmptyList(ts, tss: _*))

  def typeswitch(on: XQuery)(cases: TypeswitchCaseClause*): TypeswitchExpr =
    TypeswitchExpr(on, cases.toList)

  final case class Flwor(
    tupleStreams: IList[(String, XQuery)],
    letDefs: IList[(String, XQuery)],
    filterExpr: Option[XQuery],
    orderSpecs: IList[(XQuery, SortDirection)],
    orderIsStable: Boolean
  ) {
    def let_(d: (String, XQuery), ds: (String, XQuery)*): Flwor =
      copy(letDefs = d :: IList.fromList(ds.toList))

    def where_(expr: XQuery): Flwor =
      copy(filterExpr = Some(expr))

    def orderBy(s: (XQuery, SortDirection), ss: (XQuery, SortDirection)*): Flwor =
      copy(orderSpecs = s :: IList.fromList(ss.toList))

    def return_(expr: XQuery): XQuery = {
      val forClause = {
        val bindings = tupleStreams map {
          case (v, xqy) => s"$v in $xqy"
        } intercalate ", "

        if (tupleStreams.isEmpty) "" else s"for $bindings "
      }

      val letClause = {
        val bindings = letDefs map {
          case (v, xqy) => s"$v := $xqy"
        } intercalate ", "

        if (letDefs.isEmpty) "" else s"let $bindings "
      }

      val whereClause =
        filterExpr.map(expr => s"where $expr ").orZero

      val orderClause = {
        val specs = orderSpecs map {
          case (xqy, mod) => s"$xqy $mod"
        } intercalate ", "

        val orderKeyword = orderIsStable.fold("stable order", "order")

        if (orderSpecs.isEmpty) "" else s"$orderKeyword by $specs "
      }

      XQuery(s"${forClause}${letClause}${whereClause}${orderClause}return $expr")
    }
  }

  final case class IfExpr(cond: XQuery) {
    def then_(whenTrue: XQuery) = IfThenExpr(cond, whenTrue)
  }

  final case class IfThenExpr(cond: XQuery, whenTrue: XQuery) {
    def else_(whenFalse: XQuery): XQuery =
      XQuery(s"if ($cond) then $whenTrue else $whenFalse")
  }

  final case class TypeswitchExpr(on: XQuery, cases: List[TypeswitchCaseClause]) {
    def default(xqy: XQuery): XQuery =
      default(TypeswitchDefaultClause(None, xqy))

    def default(binding: BindingName, f: XQuery => XQuery): XQuery =
      default(TypeswitchDefaultClause(Some(binding), f(binding.xqy)))

    def default(dc: TypeswitchDefaultClause): XQuery = {
      val body = (cases.map(_.render) :+ dc.render).map("  " + _).mkString("\n")
      XQuery(s"typeswitch($on)\n$body")
    }
  }

  final case class TypeswitchCaseClause(matching: TypedBinding \/ SequenceType, result: XQuery) {
    def render: String =
      s"case ${matching.fold(_.render, _.toString)} return $result"
  }

  final case class TypeswitchDefaultClause(binding: Option[BindingName], result: XQuery) {
    def render: String = {
      val bind = binding.map(_.xqy.shows + " ")
      s"default ${~bind}return $result"
    }
  }

  sealed abstract class Quantifier {
    override def toString = this match {
      case Quantifier.Some  => "some"
      case Quantifier.Every => "every"
    }
  }

  object Quantifier {
    case object Some  extends Quantifier
    case object Every extends Quantifier
  }

  final case class QuantifiedExpr(quantifier: Quantifier, tupleStreams: NonEmptyList[(String, XQuery)]) {
    def satisfies(xqy: XQuery): XQuery = {
      val streams = tupleStreams map { case (v, seq) => s"$v in $seq" } intercalate (", ")
      XQuery(s"$quantifier $streams satisfies $xqy")
    }
  }
}
