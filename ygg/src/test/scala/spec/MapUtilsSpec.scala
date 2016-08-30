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

import ygg._, common._, data._
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

class RingDequeSpec extends quasar.Qspec {
  implicit val params = set(minTestsOk = 200, workers = java.lang.Runtime.getRuntime.availableProcessors)

  "unsafe ring deque" should {
    "implement prepend" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)

      result.toList must_=== (x +: xs)
    }

    "implement append" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)

      result.toList must_=== (xs :+ x)
    }

    "implement popFront" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)

      result.popFront() must_=== x
      result.toList must_=== xs
    }

    "implement popBack" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)

      result.popBack() must_=== x
      result.toList must_=== xs
    }

    "implement length" in prop { xs: List[Int] =>
      fromList(xs, xs.length + 10).length must_=== xs.length
      fromList(xs, xs.length).length must_=== xs.length
    }

    "satisfy identity" in prop { xs: List[Int] =>
      fromList(xs, xs.length).toList must_=== xs
    }

    "append a full list following a half-appending" in prop { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs take (xs.length / 2) foreach deque.pushBack
      (0 until (xs.length / 2)) foreach { _ =>
        deque.popFront()
      }
      xs foreach deque.pushBack
      deque.toList must_=== xs
    }

    "reverse a list by prepending" in prop { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs foreach deque.pushFront
      deque.toList must_=== xs.reverse
    }
  }

  private def fromList(xs: List[Int], bound: Int): RingDeque[Int] =
    xs.foldLeft(new RingDeque[Int](bound)) { (deque, x) =>
      deque pushBack x; deque
    }
}
