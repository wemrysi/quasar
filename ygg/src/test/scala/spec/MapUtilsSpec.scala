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

import ygg._, common._
import scalaz._, Either3._

class MapUtilsSpec extends quasar.Qspec {
  private type Ints     = List[Int]
  private type IntsPair = Ints -> Ints
  private type IntMap   = Map[Int, Ints]

  "cogroup" should {
    "produce left, right and middle cases" in prop { (left: IntMap, right: IntMap) =>
      val result = cogroup(left, right)

      val leftKeys   = left.keySet -- right.keySet
      val rightKeys  = right.keySet -- left.keySet
      val middleKeys = left.keySet & right.keySet

      val leftContrib = leftKeys.toSeq flatMap { key =>
        left(key) map { key -> left3[Int, IntsPair, Int](_) }
      }
      val rightContrib = rightKeys.toSeq flatMap { key =>
        right(key) map { key -> right3[Int, IntsPair, Int](_) }
      }
      val middleContrib = middleKeys.toSeq map { key =>
        key -> middle3[Int, IntsPair, Int]((left(key), right(key)))
      }

      result must haveSize(leftContrib.length + rightContrib.length + middleContrib.length)
    }
  }
}
