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

import ygg._, common._, table._

class CPathSpec extends quasar.Qspec {
  import CPath._

  "makeTree" should {
    "return correct tree given a sequence of CPath" in {
      val cpaths = Vec[CPath](
        "foo",
        CPath("bar", 0),
        CPath("bar", 1, "baz"),
        CPath("bar", 1, "ack"),
        CPath("bar", 2)
      )

      val values: Vec[Int] = Vec(4, 6, 7, 2, 0)
      val result           = makeTree(cpaths, values)

      val expected: CPathTree[Int] = {
        RootNode(
          Seq(
            FieldNode(
              CPathField("bar"),
              Seq(
                IndexNode(CPathIndex(0), Vec(LeafNode(4))),
                IndexNode(CPathIndex(1), Vec(FieldNode(CPathField("ack"), Seq(LeafNode(6))), FieldNode(CPathField("baz"), Seq(LeafNode(7))))),
                IndexNode(CPathIndex(2), Vec(LeafNode(2))))),
            FieldNode(CPathField("foo"), Vec(LeafNode(0)))))
      }

      result mustEqual expected
    }
  }
}
