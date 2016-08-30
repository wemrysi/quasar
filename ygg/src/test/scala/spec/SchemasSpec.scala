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

import scalaz._, Scalaz._
import ygg._, common._, json._
import ygg.json.{ JType => J }

class SchemasSpec extends ColumnarTableQspec {
  "in schemas" >> {
    "find a schema in single-schema table" in testSingleSchema
    "find a schema in homogeneous array table" in testHomogeneousArraySchema
    "find schemas separated by slice boundary" in testCrossSliceSchema
    "extract intervleaved schemas" in testIntervleavedSchema
    "don't include undefineds in schema" in testUndefinedsInSchema
    "deal with most expected types" in testAllTypesInSchema
  }

  private def testSingleSchema = {
    val expected    = Set[JType](J.Object("a" -> JNumberT, "b" -> JTextT, "c" -> JNullT))
    val trivialData = Stream.fill(100)(json"""{ "a": 1, "b": "x", "c": null }""")
    val sample      = SampleData(trivialData)
    val table       = fromSample(sample, Some(10))

    table.schemas.copoint must_=== expected
  }

  private def testHomogeneousArraySchema = {
    val expected = Set(JArrayHomogeneousT(JNumberT))
    val data     = Stream.fill(10)(json"""[1, 2, 3]""")
    val table0   = fromSample(SampleData(data), Some(10))
    val table    = table0.toArray[Long]
    table.schemas.copoint must_== expected
  }

  private def testCrossSliceSchema = {
    val expected = Set[JType](
      J.Object("a" -> JNumberT, "b" -> JTextT),
      J.Object("a" -> JTextT, "b"   -> JNumberT)
    )
    val data = Stream.fill(10)(jsonMany"""
      { "a": 1, "b": "2" }
      { "a": "x", "b": 2 }
    """).flatten

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  private def testIntervleavedSchema = {
    val expected = Set[JType](
      J.Object("a" -> J.Array(), "b" -> JTextT),
      J.Object("a" -> JNullT, "b" -> JTextT),
      J.Object("a" -> J.Array(JNumberT, JNumberT), "b" -> J.Array(JTextT, J.Object()))
    )
    val data = Stream.fill(10)(jsonMany"""
      {"a":[],"b":"2"}
      {"a":null,"b":"2"}
      {"a":[1,2],"b":["2",{}]}
    """).flatten

    data.length must_=== 30
    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  private def testUndefinedsInSchema = {
    val expected = Set(
      J.Object("a" -> JNumberT, "b" -> JNumberT),
      J.Object("a" -> JNumberT),
      J.Object("b" -> JNumberT),
      J.Object()
    )

    val data = Stream.tabulate(100) {
      case i if i % 4 == 0 => JObject(List(JField("a", JNum(1)), JField("b", JNum(i))))
      case i if i % 4 == 1 => JObject(List(JField("a", JNum(1)), JField("b", JUndefined)))
      case i if i % 4 == 2 => JObject(List(JField("a", JUndefined), JField("b", JNum(i))))
      case _ => JObject()
    }

    val table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_== expected
  }

  private def testAllTypesInSchema = {
    val data: Stream[JValue] = jsonMany"""
      1
      true
      null
      "abc"
      [ 1, 2 ]
      { "a": 1 }
      { "a": true }
      { "a": null }
      { "a": "a" }
      { "a": 1.2 }
      { "a": 112311912931223e-1000 }
      { "a": [] }
      { "a": {} }
      { "a": [ 1, "a", true ] }
      { "a": { "b": { "c": 3 } } }
    """.toStream

    val expected = Set[JType](
      JNumberT,
      JTextT,
      JBooleanT,
      JNullT,
      J.Array(JNumberT, JNumberT),
      J.Object("a" -> JNumberT),
      J.Object("a" -> JBooleanT),
      J.Object("a" -> JTextT),
      J.Object("a" -> JNullT),
      J.Object("a" -> J.Array()),
      J.Object("a" -> J.Object()),
      J.Object("a" -> J.Array(JNumberT, JTextT, JBooleanT)),
      J.Object("a" -> J.Object("b" -> J.Object("c" -> JNumberT)))
    )

    val table: Table = fromSample(SampleData(data), Some(10))
    table.schemas.copoint must_=== expected
  }
}
