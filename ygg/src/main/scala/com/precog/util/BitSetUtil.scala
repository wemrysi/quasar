package blueeyes

object BitSetUtil {
  def fromArray(arr: Array[Long]): BitSet = doto(create())(_ setBits arr)
  def create(): BitSet                    = new BitSet()
  def create(ns: Seq[Int]): BitSet        = doto(create())(bs => ns foreach (bs set _))
  def range(start: Int, end: Int): BitSet = doto(create())(bs => Loop.range(start, end)(bs set _))

  def filteredRange(start: Int, end: Int)(pred: Int => Boolean): BitSet =
    doto(create())(bs => Loop.range(start, end)(i => if (pred(i)) bs set i))

  def filteredRange(r: Range)(pred: Int => Boolean): BitSet =
    filteredRange(r.start, r.end)(pred)
}
