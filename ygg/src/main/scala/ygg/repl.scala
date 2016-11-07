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

package ygg

import common._, macros._
import json._, table._, trans._
import scalaz._
import TableData.fromJValues

object repl {
  lazy val bigZips   = TableData.fromFile(new jFile("it/src/main/resources/tests/zips.data"))
  lazy val smallZips = TableData.fromFile(new jFile("it/src/main/resources/tests/smallZips.data"))

  implicit final class JvalueInterpolator(sc: StringContext) {
    def json(args: Any*): JValue             = macro JValueMacros.jsonInterpolatorImpl
    def jsonMany(args: Any*): Vector[JValue] = macro JValueMacros.jsonManyInterpolatorImpl
  }

  def medals      = fromJValues(medalsIn)
  def medalsMerge = MergeTable(grouping)(evaluator)

  def medalsIn = jsonMany"""
    {"key":[5908438637678314371],"value":{"Edition":"2000","Gender":"Men"}}
    {"key":[5908438637678314372],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314373],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314374],"value":{"Edition":"2004","Gender":"Women"}}
    {"key":[5908438637678314375],"value":{"Edition":"2000","Gender":"Women"}}
    {"key":[5908438637678314376],"value":{"Edition":"1996","Gender":"Women"}}
    {"key":[5908438637678314377],"value":{"Edition":"2008","Gender":"Men"}}
    {"key":[5908438637678314378],"value":{"Edition":"2004","Gender":"Men"}}
    {"key":[5908438637678314379],"value":{"Edition":"1996","Gender":"Men"}}
    {"key":[5908438637678314380],"value":{"Edition":"2008","Gender":"Women"}}
  """.toStream

  def medalsOut = jsonMany"""
    {"key":[],"value":{"year":"1996","ratio":139.0}}
    {"key":[],"value":{"year":"2000","ratio":126.0}}
    {"key":[],"value":{"year":"2004","ratio":122.0}}
    {"key":[],"value":{"year":"2008","ratio":119.0}}
  """


  def grouping = {
    def genderFilter(str: String) = Filter(EqualLiteral(dotValue.dyn.Gender, CString(str), false))
    def targetTrans = InnerObjectConcat(
      root delete "value",
      dotValue.dyn.Gender wrapObjectField "value"
    )
    def mkSource(groupId: Int, key: String, value: String) = GroupingSource(
      medals.asRep,
      medals,
      dotKey,
      Some(targetTrans),
      groupId = groupId,
      GroupKeySpecSource(key, genderFilter(value)) && GroupKeySpecSource("1" -> dotValue.dyn.Edition)
    )
    GroupingAlignment.intersect(
      mkSource(0, "extra0", "Men"),
      mkSource(2, "extra1", "Women")
    )
  }
  def evaluator(key: RValue, partition: GroupId => Need[TableData]) = {
    val K0 = RValue.fromJValue(json"""{"1":"1996","extra0":true,"extra1":true}""")
    val K1 = RValue.fromJValue(json"""{"1":"2000","extra0":true,"extra1":true}""")
    val K2 = RValue.fromJValue(json"""{"1":"2004","extra0":true,"extra1":true}""")
    val K3 = RValue.fromJValue(json"""{"1":"2008","extra0":true,"extra1":true}""")

    val r0 = fromJValues(jsonMany"""{"key":[],"value":{"year":"1996","ratio":139.0}}""")
    val r1 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2000","ratio":126.0}}""")
    val r2 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2004","ratio":122.0}}""")
    val r3 = fromJValues(jsonMany"""{"key":[],"value":{"year":"2008","ratio":119.0}}""")

    Need {
      key match {
        case K0 => r0
        case K1 => r1
        case K2 => r2
        case K3 => r3
        case _  => abort("Unexpected group key")
      }
    }
  }

  implicit class TableSelectionOps[T](val rep: TableRep[T]) {
    import rep._

    private implicit def nextOps(next: T): TableSelectionOps[T] = TableSelectionOps[T](TableRep(next, companion))

    private type F1 = trans.TransSpec[trans.Source.type]

    def p(): Unit                = table.dump()
    def map(f: TransSpec1): T    = table transform f
    def filter(p: F1): T         = map(root filter p)
    def delete(p: JPathField): T = map(root delete CPathField(p.name))

    def filterAt[A: CValueType](select: F1, literal: A): T = filter(EqualLiteral(select, literal, invert = false))

    def \(path: JPath): T  = path.nodes.foldLeft(table)(_ \ _)
    def \(path: String): T = this \ JPath(path)
    def \(node: JPathNode): T = node match {
      case JPathField(name) => map(root select name)
      case JPathIndex(idx)  => map(root apply idx)
    }

    def --(fields: Iterable[JPathField]): T = fields.foldLeft(table)(_ delete _)
  }
}
