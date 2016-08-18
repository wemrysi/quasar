package com.precog.util

import blueeyes._
import scalaz._, Ordering._

sealed trait IntList {
  def head: Int
  def tail: IntList
}
final case class IntCons(head: Int, tail: IntList) extends IntList
final case object IntNil extends IntList {
  def head = Nil.head
  def tail = this
}

object IntList {
  def empty: IntList           = IntNil
  def apply(xs: Int*): IntList = xs.foldRight(empty)(_ :: _)

  implicit class IntListOps(private val xs: IntList) extends AnyVal {
    def ::(head: Int): IntCons = IntCons(head, xs)
    @tailrec final def foreach(f: Int => Any): Unit = xs match {
      case IntCons(hd, tl) => f(hd); tl foreach f
      case _               =>
    }
  }
}

object NumericComparisons {

  @inline def compare(a: Long, b: Long): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Long, b: Double): Int = -compare(b, a)

  @inline def compare(a: Long, b: BigDecimal): Int = BigDecimal(a) compare b

  def compare(a: Double, bl: Long): Int = {
    val b = bl.toDouble
    if (b.toLong == bl) {
      if (a < b) -1 else if (a == b) 0 else 1
    } else {
      val error = math.abs(b * 2.220446049250313E-16)
      if (a < b - error) -1 else if (a > b + error) 1 else bl.signum
    }
  }

  @inline def compare(a: Double, b: Double): Int = if (a < b) -1 else if (a == b) 0 else 1

  @inline def compare(a: Double, b: BigDecimal): Int = BigDecimal(a) compare b

  @inline def compare(a: BigDecimal, b: Long): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: Double): Int = a compare BigDecimal(b)

  @inline def compare(a: BigDecimal, b: BigDecimal): Int = a compare b

  @inline def compare(a: DateTime, b: DateTime): Int = {
    val res: Int = a compareTo b
    if (res < 0) -1
    else if (res > 0) 1
    else 0
  }

  @inline def eps(b: Double): Double = math.abs(b * 2.220446049250313E-16)

  def approxCompare(a: Double, b: Double): Int = {
    val aError = eps(a)
    val bError = eps(b)
    if (a + aError < b - bError) -1 else if (a - aError > b + bError) 1 else 0
  }

  @inline def order(a: Long, b: Long): Cmp             = if (a < b) LT else if (a == b) EQ else GT
  @inline def order(a: Double, b: Double): Cmp         = if (a < b) LT else if (a == b) EQ else GT
  @inline def order(a: Long, b: Double): Cmp           = Cmp(compare(a, b))
  @inline def order(a: Double, b: Long): Cmp           = Cmp(compare(a, b))
  @inline def order(a: Long, b: BigDecimal): Cmp       = Cmp(compare(a, b))
  @inline def order(a: Double, b: BigDecimal): Cmp     = Cmp(compare(a, b))
  @inline def order(a: BigDecimal, b: Long): Cmp       = Cmp(compare(a, b))
  @inline def order(a: BigDecimal, b: Double): Cmp     = Cmp(compare(a, b))
  @inline def order(a: BigDecimal, b: BigDecimal): Cmp = Cmp(compare(a, b))
  @inline def order(a: DateTime, b: DateTime): Cmp     = Cmp(compare(a, b))
}

/**
  * This class exists as a replacement for Unit in Unit-returning functions.
  * The main issue with unit is that the coercion of any return value to
  * unit means that we were sometimes masking mis-returns of functions. In
  * particular, functions returning IO[Unit] would happily coerce IO => Unit,
  * which essentially discarded the inner IO work.
  */
sealed trait PrecogUnit

object PrecogUnit extends PrecogUnit {
  implicit def liftUnit(unit: Unit): PrecogUnit = this
}

/**
  * Unchecked and unboxed (fast!) deque implementation with a fixed bound.  None
  * of the operations on this datastructure are checked for bounds.  You are
  * trusted to get this right on your own.  If you do something weird, you could
  * end up overwriting data, reading old results, etc.  Don't do that.
  *
  * No objects were allocated in the making of this film.
  */
final class RingDeque[@specialized(Boolean, Int, Long, Double, Float, Short) A: CTag](_bound: Int) {
  val bound = _bound + 1

  private val ring  = new Array[A](bound)
  private var front = 0
  private var back  = rotate(front, 1)

  def isEmpty = front == rotate(back, -1)

  def popFront(): A = {
    val result = ring(front)
    moveFront(1)
    result
  }

  def pushFront(a: A) {
    moveFront(-1)
    ring(front) = a
  }

  def popBack(): A = {
    moveBack(-1)
    ring(rotate(back, -1))
  }

  def pushBack(a: A) {
    ring(rotate(back, -1)) = a
    moveBack(1)
  }

  def length: Int =
    (if (back > front) back - front else (back + bound) - front) - 1

  def toList: List[A] = {
    @tailrec def loop(i: Int, accum: List[A]): List[A] =
      if (i < front) accum else loop(i - 1, ring(i % bound) :: accum)

    loop(front + length - 1, Nil)
  }

  private[this] def rotate(target: Int, delta: Int) = (target + delta + bound) % bound
  private[this] def moveFront(delta: Int): Unit     = front = rotate(front, delta)
  private[this] def moveBack(delta: Int): Unit      = back = rotate(back, delta)
}
