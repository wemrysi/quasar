package ygg.data

import blueeyes._

object Bits {
  def apply(): BitSet                     = new BitSet()
  def apply(ns: Seq[Int]): BitSet         = doto(apply())(bs => ns foreach (bs set _))
  def fromArray(arr: Array[Long]): BitSet = doto(apply())(_ setBits arr)
  def range(start: Int, end: Int): BitSet = doto(apply())(bs => Loop.range(start, end)(bs set _))

  def filteredRange(start: Int, end: Int)(pred: Int => Boolean): BitSet =
    doto(apply())(bs => Loop.range(start, end)(i => if (pred(i)) bs set i))

  def filteredRange(r: Range)(pred: Int => Boolean): BitSet =
    filteredRange(r.start, r.end)(pred)
}
