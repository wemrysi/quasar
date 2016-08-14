package com.precog.util

import blueeyes._
// import quasar.precog.TestSupport._
import scalaz._

object MapUtilsSpec extends quasar.QuasarSpecification with MapUtils {
  private type Ints     = List[Int]
  private type IntsPair = Ints -> Ints
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

object RingDequeSpec extends quasar.QuasarSpecification {
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



object VectorClockSpec extends quasar.QuasarSpecification {
  "vector clock" should {
    "update when key not already present" in {
      val vc = VectorClock.empty
      vc.update(1,1) must_== VectorClock(Map((1 -> 1)))
    }
    "update when key is present and new value is greater" in {
      val vc = VectorClock(Map((1 -> 0)))
      vc.update(1,1) must_== VectorClock(Map((1 -> 1)))
    }
    "not update when key is present and new value is lesser" in {
      val vc = VectorClock(Map((1 -> 2)))
      vc.update(1,1) must_== vc
    }
    "evaluate lower bound for single key" in {
      val vc1 = VectorClock(Map(0 -> 0))
      val vc2 = VectorClock(Map(0 -> 1))
      val vc3 = VectorClock(Map(0 -> 2))

      vc2.isDominatedBy(vc1) must beFalse
      vc2.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc3) must beTrue
    }
    "evaluate lower bound for multiple single keys" in {
      val vc1 = VectorClock(Map(0 -> 0, 1->1))
      val vc2 = VectorClock(Map(0 -> 1, 1->2))
      val vc3 = VectorClock(Map(0 -> 2, 1->0))

      vc2.isDominatedBy(vc1) must beFalse
      vc2.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc3) must beFalse
    }
    "evaluate lower bound with missing keys" in {
      val vc1 = VectorClock(Map((0->1),(1->0)))
      val vc2 = VectorClock(Map((1->0),(2->1)))
      val vc3 = VectorClock(Map((1->1),(3->1)))

      vc1.isDominatedBy(vc2) must beTrue
      vc2.isDominatedBy(vc1) must beTrue
      vc3.isDominatedBy(vc1) must beFalse
    }
    "evaluate lower bound with respect to empty" in {
      val vc = VectorClock(Map(0 -> 0, 1->1))

      VectorClock.empty.isDominatedBy(vc) must beTrue
      vc.isDominatedBy(VectorClock.empty) must beTrue
    }
  }
}
