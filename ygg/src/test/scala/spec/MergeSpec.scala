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
import ygg._, common._, table._, trans._

class MergeSpec extends TableQspec {
  "merge" should {
    "avoid crosses in trivial cases" in {
      val foo = fromJson(jsonMany"""
        {"key":[5908438637678328470],"value":{"a":0,"b":4}}
        {"key":[5908438637678328471],"value":{"a":1,"b":5}}
        {"key":[5908438637678328472],"value":{"a":2,"b":6}}
        {"key":[5908438637678328473],"value":{"a":3,"b":7}}
      """.toStream)

      val bar = fromJson(jsonMany"""
        {"key":[5908438637678328576],"value":{"a":-1,"c":8,"b":-1}}
        {"key":[5908438637678328577],"value":{"a":1,"c":9,"b":-1}}
        {"key":[5908438637678328578],"value":{"a":-1,"c":10,"b":6}}
        {"key":[5908438637678328579],"value":{"a":3,"c":11,"b":7}}
        {"key":[5908438637678328580],"value":{"a":0,"c":12,"b":-1}}
        {"key":[5908438637678328581],"value":{"a":0,"c":13,"b":-1}}
      """.toStream)

      val resultJson = jsonMany"""
        {"key":[5908438637678328470,5908438637678328580],"value":{"b":4,"c":12,"a":0,"fa":{"b":4,"a":0}}}
        {"key":[5908438637678328470,5908438637678328581],"value":{"b":4,"c":13,"a":0,"fa":{"b":4,"a":0}}}
        {"key":[5908438637678328471,5908438637678328577],"value":{"b":5,"c":9,"a":1,"fa":{"b":5,"a":1}}}
        {"key":[5908438637678328472,5908438637678328578],"value":{"b":6,"c":10,"a":2,"fa":{"b":6,"a":2}}}
        {"key":[5908438637678328473,5908438637678328579],"value":{"b":7,"c":11,"a":3,"fa":{"b":7,"a":3}}}
      """

      val valueField = CPathField("value")
      val oneField   = CPathField("1")
      val twoField   = CPathField("2")

      val grouping =
        GroupingAlignment(
          TransSpec1.Id,
          TransSpec1.Id,
          GroupingSource(
            bar,
            root.key,
            Some(
              InnerObjectConcat(
                ObjectDelete(root, Set(valueField)),
                WrapObject(root.value.c, "value"))),
            0,
            GroupKeySpecOr(
              GroupKeySpecSource(oneField, root.value.a),
              GroupKeySpecSource(twoField, root.value.b))
          ),
          GroupingSource(
            foo,
            root.key,
            Some(InnerObjectConcat(ObjectDelete(root, Set(valueField)), WrapObject(root.value, "value"))),
            3,
            GroupKeySpecAnd(
              GroupKeySpecSource(oneField, root.value.a),
              GroupKeySpecSource(twoField, root.value.b))
          ),
          GroupingSpec.Intersection)

      def evaluator(key: RValue, partition: GroupId => NeedTable) = {
        val K0 = RValue.fromJValue(json"""{"1":0,"2":4}""")
        val K1 = RValue.fromJValue(json"""{"1":1,"2":5}""")
        val K2 = RValue.fromJValue(json"""{"1":2,"2":6}""")
        val K3 = RValue.fromJValue(json"""{"1":3,"2":7}""")

        val r0 = fromJson(jsonMany"""
          {"key":[5908438637678328470,5908438637678328580],"value":{"b":4,"c":12,"a":0,"fa":{"b":4,"a":0}}}
          {"key":[5908438637678328470,5908438637678328581],"value":{"b":4,"c":13,"a":0,"fa":{"b":4,"a":0}}}
        """.toStream)

        val r1 = fromJson(jsonMany"""
          {"key":[5908438637678328471,5908438637678328577],"value":{"b":5,"c":9,"a":1,"fa":{"b":5,"a":1}}}
        """.toStream)

        val r2 = fromJson(jsonMany"""
          {"key":[5908438637678328472,5908438637678328578],"value":{"b":6,"c":10,"a":2,"fa":{"b":6,"a":2}}}
        """.toStream)

        val r3 = fromJson(jsonMany"""
          {"key":[5908438637678328473,5908438637678328579],"value":{"b":7,"c":11,"a":3,"fa":{"b":7,"a":3}}}
        """.toStream)

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

      val result = Table.merge(grouping)(evaluator)
      result.flatMap(_.toJson).copoint.toSet must_== resultJson.toSet
    }

    "execute the medals query without a cross" in {
      val medals = fromJson(jsonMany"""
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
      """.toStream)

      val resultJson = jsonMany"""
        {"key":[],"value":{"year":"1996","ratio":139.0}}
        {"key":[],"value":{"year":"2000","ratio":126.0}}
        {"key":[],"value":{"year":"2004","ratio":122.0}}
        {"key":[],"value":{"year":"2008","ratio":119.0}}
      """.toStream

      val valueField = CPathField("value")
      val oneField   = CPathField("1")

      val grouping =
        GroupingAlignment(
          TransSpec1.Id,
          TransSpec1.Id,
          GroupingSource(
            medals,
            root.key,
            Some(
              InnerObjectConcat(
                ObjectDelete(root, Set(valueField)),
                WrapObject(root.value.Gender, "value"))),
            0,
            GroupKeySpecAnd(
              GroupKeySpecSource(
                CPathField("extra0"),
                Filter(
                  EqualLiteral(root.value.Gender, CString("Men"), false),
                  EqualLiteral(root.value.Gender, CString("Men"), false))),
              GroupKeySpecSource(oneField, root.value.Edition))
          ),
          GroupingSource(
            medals,
            root.key,
            Some(
              InnerObjectConcat(
                ObjectDelete(root, Set(valueField)),
                WrapObject(root.value.Gender, "value"))),
            2,
            GroupKeySpecAnd(
              GroupKeySpecSource(
                CPathField("extra1"),
                Filter(
                  EqualLiteral(root.value.Gender, CString("Women"), false),
                  EqualLiteral(root.value.Gender, CString("Women"), false))),
              GroupKeySpecSource(oneField, root.value.Edition))
          ),
          GroupingSpec.Intersection)

      def evaluator(key: RValue, partition: GroupId => NeedTable) = {
        val K0 = RValue.fromJValue(json"""{"1":"1996","extra0":true,"extra1":true}""")
        val K1 = RValue.fromJValue(json"""{"1":"2000","extra0":true,"extra1":true}""")
        val K2 = RValue.fromJValue(json"""{"1":"2004","extra0":true,"extra1":true}""")
        val K3 = RValue.fromJValue(json"""{"1":"2008","extra0":true,"extra1":true}""")

        val r0 = fromJson(jsonMany"""{"key":[],"value":{"year":"1996","ratio":139.0}}""".toStream)
        val r1 = fromJson(jsonMany"""{"key":[],"value":{"year":"2000","ratio":126.0}}""".toStream)
        val r2 = fromJson(jsonMany"""{"key":[],"value":{"year":"2004","ratio":122.0}}""".toStream)
        val r3 = fromJson(jsonMany"""{"key":[],"value":{"year":"2008","ratio":119.0}}""".toStream)

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

      val result = Table.merge(grouping)(evaluator)
      result.flatMap(_.toJson).copoint.toSet must_== resultJson.toSet
    }
  }
}
