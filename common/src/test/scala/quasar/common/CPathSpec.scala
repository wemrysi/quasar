/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.common

import slamdata.Predef._

import quasar.pkg.tests._

class CPathSpec extends Specification {
  import CPath._

  "makeTree" should {
    "return correct tree given a sequence of CPath" in {
      val cpaths: List[CPath] = List(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz")),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("ack")),
        CPath(CPathField("bar"), CPathIndex(2)))

      val values: List[Int] = List(4, 6, 7, 2, 0)

      val result = makeTree(cpaths, values)

      val expected: CPathTree[Int] = {
        RootNode(List(
          FieldNode(CPathField("bar"),
            List(
              IndexNode(CPathIndex(0), List(LeafNode(4))),
              IndexNode(CPathIndex(1),
                List(
                  FieldNode(CPathField("ack"), List(LeafNode(6))),
                  FieldNode(CPathField("baz"), List(LeafNode(7))))),
              IndexNode(CPathIndex(2), List(LeafNode(2))))),
          FieldNode(CPathField("foo"), List(LeafNode(0)))))
      }

      result mustEqual expected
    }
  }
}
