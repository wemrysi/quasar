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

package quasar.precog.util

import quasar.pkg.tests._

import scalaz._

import java.io.File

class IOUtilsSpecs extends Specification {
  "IOUtils" should {
    "properly clean empty directories recursively" in {
      val tmpRoot = IOUtils.createTmpDir("IOUtilsSpecs").unsafePerformIO

      val rootParent = tmpRoot.getParentFile

      val child1 = new File(tmpRoot, "child1")
      val child2 = new File(tmpRoot, "child2")

      val gchild1_1 = new File(child1, "child1")
      val gchild1_2 = new File(child1, "child2")

      val gchild2_1 = new File(child2, "child1")

      List(child1, child2, gchild1_1, gchild1_2, gchild2_1).foreach(_.mkdir)

      // This should fail because tmpRoot has children
      IOUtils.recursiveDeleteEmptyDirs(tmpRoot, rootParent).unsafePerformIO

      tmpRoot.isDirectory mustEqual true

      // This should delete both gchild2_1 and child2, but leave tmpRoot
      IOUtils.recursiveDeleteEmptyDirs(gchild2_1, rootParent).unsafePerformIO

      gchild2_1.isDirectory mustEqual false
      child2.isDirectory mustEqual false
      tmpRoot.isDirectory mustEqual true

      // This should delete gchild1_1 and leave child1 and gchild1_2
      IOUtils.recursiveDeleteEmptyDirs(gchild1_1, rootParent).unsafePerformIO

      gchild1_1.isDirectory mustEqual false
      gchild1_2.isDirectory mustEqual true
      child1.isDirectory mustEqual true
      tmpRoot.isDirectory mustEqual true

      // This should not do anything because it's specified as the root
      IOUtils.recursiveDeleteEmptyDirs(tmpRoot, tmpRoot).unsafePerformIO

      tmpRoot.isDirectory mustEqual true

      // cleanup
      IOUtils.recursiveDelete(tmpRoot).unsafePerformIO

      tmpRoot.isDirectory mustEqual false
    }
  }
}

object MapUtilsSpecs extends Specification with ScalaCheck with MapUtils {
  private type Ints     = List[Int]
  private type IntsPair = (Ints, Ints)
  private type IntMap   = Map[Int, Ints]

  "cogroup" should {
    "produce left, right and middle cases" in skipped(prop { (left: IntMap, right: IntMap) =>

      val result     = left cogroup right
      val leftKeys   = left.keySet -- right.keySet
      val rightKeys  = right.keySet -- left.keySet
      val middleKeys = left.keySet & right.keySet

      val leftContrib = leftKeys.toSeq flatMap { key =>
        left(key) map { key -> Either3.left3[Int, IntsPair, Int](_) }
      }

      val rightContrib = rightKeys.toSeq flatMap { key =>
        right(key) map { key -> Either3.right3[Int, IntsPair, Int](_) }
      }

      val middleContrib = middleKeys.toSeq map { key =>
        key -> Either3.middle3[Int, IntsPair, Int]((left(key), right(key)))
      }

      result must containAllOf(leftContrib)
      result must containAllOf(rightContrib)
      result must containAllOf(middleContrib)
      result must haveSize(leftContrib.length + rightContrib.length + middleContrib.length)
    })
  }
}

object RingDequeSpecs extends Specification with ScalaCheck {
  implicit val params = set(
    minTestsOk = 2500,
    workers = Runtime.getRuntime.availableProcessors)

  "unsafe ring deque" should {
    "implement prepend" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)

      result.toList mustEqual (x +: xs)
    }

    "implement append" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)

      result.toList mustEqual (xs :+ x)
    }

    "implement popFront" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)

      result.popFront() mustEqual x
      result.toList mustEqual xs
    }

    "implement popBack" in prop { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)

      result.popBack() mustEqual x
      result.toList mustEqual xs
    }

    "implement length" in prop { xs: List[Int] =>
      fromList(xs, xs.length + 10).length mustEqual xs.length
      fromList(xs, xs.length).length mustEqual xs.length
    }

    "satisfy identity" in prop { xs: List[Int] =>
      fromList(xs, xs.length).toList mustEqual xs
    }

    "append a full list following a half-appending" in prop { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs take (xs.length / 2) foreach deque.pushBack
      (0 until (xs.length / 2)) foreach { _ => deque.popFront() }
      xs foreach deque.pushBack
      deque.toList mustEqual xs
    }

    "reverse a list by prepending" in prop { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs foreach deque.pushFront
      deque.toList mustEqual xs.reverse
    }
  }

  private def fromList(xs: List[Int], bound: Int): RingDeque[Int] =
    xs.foldLeft(new RingDeque[Int](bound)) { (deque, x) => deque pushBack x; deque }
}
