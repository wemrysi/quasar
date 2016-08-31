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

import scalaz._

sealed abstract class XQuery {
  import XQuery._

  def apply(predicate: XQuery): XQuery = XQuery(s"$this[$predicate]")
  def unary_- : XQuery = XQuery(s"-$this")
  def -(other: XQuery): XQuery = XQuery(s"$this - $other")
  def +(other: XQuery): XQuery = XQuery(s"$this + $other")
  def *(other: XQuery): XQuery = XQuery(s"$this * $other")
  def div(other: XQuery): XQuery = XQuery(s"$this div $other")
  def idiv(other: XQuery): XQuery = XQuery(s"$this idiv $other")
  def mod(other: XQuery): XQuery = XQuery(s"$this mod $other")
  def and(other: XQuery): XQuery = XQuery(s"$this and $other")
  def or(other: XQuery): XQuery = XQuery(s"$this or $other")
  def seq: XQuery = mkSeq_(this)
  def to(upper: XQuery): XQuery = XQuery(s"$this to $upper")

  def `/`(xqy: XQuery): XQuery = xqy match {
    case StringLit(s)   => XQuery(s"$this/$s")
    case Expression(ex) => XQuery(s"""$this/xdmp:value("$ex")""")
  }

  def `//`(xqy: XQuery): XQuery = xqy match {
    case StringLit(s)   => XQuery(s"$this//$s")
    case Expression(ex) => XQuery(s"""$this//xdmp:value("$ex")""")
  }

  // Value Comparisons
  def eq(other: XQuery): XQuery = XQuery(s"$this eq $other")
  def ne(other: XQuery): XQuery = XQuery(s"$this ne $other")
  def lt(other: XQuery): XQuery = XQuery(s"$this lt $other")
  def le(other: XQuery): XQuery = XQuery(s"$this le $other")
  def gt(other: XQuery): XQuery = XQuery(s"$this gt $other")
  def ge(other: XQuery): XQuery = XQuery(s"$this ge $other")

  // General Comparisons
  def ===(other: XQuery): XQuery = XQuery(s"$this = $other")
  def =/=(other: XQuery): XQuery = XQuery(s"$this != $other")
  def <(other: XQuery): XQuery = XQuery(s"$this < $other")
  def <=(other: XQuery): XQuery = XQuery(s"$this <= $other")
  def >(other: XQuery): XQuery = XQuery(s"$this > $other")
  def >=(other: XQuery): XQuery = XQuery(s"$this >= $other")

  // Node Comparisons
  def is(other: XQuery): XQuery = XQuery(s"$this is $other")
  def <<(other: XQuery): XQuery = XQuery(s"$this << $other")
  def >>(other: XQuery): XQuery = XQuery(s"$this >> $other")

  // TODO: Accept a list of namespaces? Or somehow gather namespaces used in
  //       constructing the query in order to be able to output them here.
  def toQuery: String =
    s"""
      xquery version "1.0-ml";
      import module namespace json = "http://marklogic.com/xdmp/json" at "/MarkLogic/json/json.xqy";
      declare namespace ejson  = "http://quasar-analytics.org/ejson";
      declare namespace quasar = "http://quasar-analytics.org/quasar";

      $this
    """
}

object XQuery {
  final case class StringLit(str: String) extends XQuery {
    override def toString = s""""$str""""
  }
  final case class Expression(override val toString: String) extends XQuery

  def apply(expr: String): XQuery =
    Expression(expr)

  implicit val show: Show[XQuery] =
    Show.showFromToString
}
