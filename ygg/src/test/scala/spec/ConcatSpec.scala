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

import ygg._, common._, json._, table._
import trans._
import CPath._

class ConcatSpec extends TableQspec {
  "in concat" >> {
    "concat two tables" in testConcat
    "transform a CPathTree into a TransSpec" in testTreeToTransSpec
  }

  private def testConcat = {
    val data1: Stream[JValue] = Stream.fill(25)(json"""{ "a": 1, "b": "x", "c": null }""")
    val data2: Stream[JValue] = Stream.fill(35)(json"""[4, "foo", null, true]""")

    val table1   = fromSample(SampleData(data1), Some(10))
    val table2   = fromSample(SampleData(data2), Some(10))
    val results  = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.value must_=== expected
  }

  private def testTreeToTransSpec = {
    val tree: CPathTree[Int] = RootNode(
      Seq(
        FieldNode(
          CPathField("bar"),
          Seq(
            IndexNode(CPathIndex(0), Seq(LeafNode(4))),
            IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),
            IndexNode(CPathIndex(2), Seq(LeafNode(2))))),
        FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

    val result = TransSpec.concatChildren(tree, Leaf(Source))

    val expected = InnerObjectConcat(
      WrapObject(
        InnerArrayConcat(
          InnerArrayConcat(
            WrapArray(root(4)),
            WrapArray(WrapObject(root(6), "baz"))
          ),
          WrapArray(root(2))
        ),
        "bar"
      ),
      WrapObject(root(0), "foo")
    )

    result mustEqual expected
  }
}
