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

package ygg.tests

import scalaz.{ Source => _, _ }, Scalaz._
import ygg._, common._, json._, table._, trans._

class IndicesSpec extends TableQspec {
  def groupkey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))
  def valuekey(s: String) = DerefObjectStatic(Leaf(Source), CPathField(s))

  "a table index" should {
    "handle empty tables" in {
      val table = fromJson(Stream.empty[JValue])

      val keySpecs = Array(groupkey("a"), groupkey("b"))
      val valSpec  = valuekey("c")

      val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).copoint

      index.getUniqueKeys(0).size must_== 0
      index.getSubTable(Array(0), Array(CString("a"))).size == ExactSize(0)
    }

    val json = jsonMany"""
      {"a": 1, "b": 2, "c": 3}
      {"a": 1, "b": 2, "c": 999, "d": "foo"}
      {"a": 1, "b": 2, "c": "cat"}
      {"a": 1, "b": 2}
      {"a": 2, "b": 2, "c": 3, "d": 1248}
      {"a": 2, "b": 2, "c": 13}
      {"a": "foo", "b": "bar", "c": 3}
      {"a": 3, "b": "", "c": 333}
      {"a": 3, "b": 2, "c": [1,2,3,4]}
      {"a": 1, "b": 2, "c": {"cat": 13, "dog": 12}}
      {"a": "foo", "b": 999}
      {"b": 2, "c": 9876}
      {"a": 1, "c": [666]}
    """

    val table             = fromJson(json.toStream)
    val keySpecs          = Array(groupkey("a"), groupkey("b"))
    val valSpec           = valuekey("c")
    val index: TableIndex = TableIndex.createFromTable(table, keySpecs, valSpec).copoint

    "determine unique groupkey values" in {
      index.getUniqueKeys(0) must_== Set[RValue](CLong(1), CLong(2), CLong(3), CString("foo"))
      index.getUniqueKeys(1) must_== Set[RValue](CLong(2), CLong(999), CString("bar"), CString(""))
    }

    "determine unique groupkey sets" in {
      index.getUniqueKeys() must_== Set[scSeq[RValue]](
        Array(CLong(1), CLong(2)),
        Array(CLong(2), CLong(2)),
        Array(CString("foo"), CString("bar")),
        Array(CLong(3), CString("")),
        Array(CLong(3), CLong(2)),
        Array(CString("foo"), CLong(999))
      )
    }

    def subtableSet(index: TableIndex, ids: scSeq[Int], vs: scSeq[RValue]): Set[RValue] =
      index.getSubTable(ids, vs).toJson.copoint.toSet.map(RValue.fromJValue)

    def test(vs: scSeq[RValue], result: Set[RValue]) =
      subtableSet(index, Array(0, 1), vs) must_== result

    "generate subtables based on groupkeys" in {
      def empty = Set.empty[RValue]

      test(Array(CLong(1), CLong(1)), empty)

      test(Array(CLong(1), CLong(2)), s1)
      def s1 = Set[RValue](
        CLong(3),
        CLong(999),
        CString("cat"),
        RObject(Map("cat" -> CLong(13), "dog" -> CLong(12)))
      )

      test(Array(CLong(2), CLong(2)), s2)
      def s2 = Set[RValue](CLong(3), CLong(13))

      test(Array(CString("foo"), CString("bar")), s3)
      def s3 = Set[RValue](CLong(3))

      test(Array(CLong(3), CString("")), s4)
      def s4 = Set[RValue](CLong(333))

      test(Array(CLong(3), CLong(2)), s5)
      def s5 = Set[RValue](RArray(CLong(1), CLong(2), CLong(3), CLong(4)))

      test(Array(CString("foo"), CLong(999)), empty)
    }

    val index1 = TableIndex
      .createFromTable(
        table,
        Array(groupkey("a")),
        valuekey("c")
      )
      .copoint

    val index2 = TableIndex
      .createFromTable(
        table,
        Array(groupkey("b")),
        valuekey("c")
      )
      .copoint

    "efficiently combine to produce unions" in {

      def tryit(tpls: (TableIndex, scSeq[Int], Seq[RValue])*)(expected: JValue*) = {
        val table = TableIndex.joinSubTables(tpls.toList)
        table.toJson.copoint.toSet must_== expected.toSet
      }

      // both disjunctions have data
      tryit(
        (index1, Seq(0), Seq(CLong(1))),
        (index2, Seq(0), Seq(CLong(2)))
      )(
        JNum(3),
        JNum(999),
        JNum(9876),
        JString("cat"),
        JNum(13),
        JArray(JNum(1), JNum(2), JNum(3), JNum(4)),
        JArray(JNum(666)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // only first disjunction has data
      tryit(
        (index1, Seq(0), Seq(CLong(1))),
        (index2, Seq(0), Seq(CLong(1234567)))
      )(
        JNum(3),
        JNum(999),
        JString("cat"),
        JArray(JNum(666)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // only second disjunction has data
      tryit(
        (index1, Seq(0), Seq(CLong(-8000))),
        (index2, Seq(0), Seq(CLong(2)))
      )(
        JNum(3),
        JNum(999),
        JNum(9876),
        JString("cat"),
        JNum(13),
        JArray(JNum(1), JNum(2), JNum(3), JNum(4)),
        JObject(Map("cat" -> JNum(13), "dog" -> JNum(12)))
      )

      // neither disjunction has data
      tryit(
        (index1, Seq(0), Seq(CLong(-8000))),
        (index2, Seq(0), Seq(CLong(1234567)))
      )()
    }
  }
}
