package ygg.data

import ygg.common._

class ArrayIntList(initialCapacity: Int) {
  private[this] var _size: Int        = 0
  private[this] var _data: Array[Int] = new Array[Int](initialCapacity)

  def this()                              = this(8)
  def size(): Int                         = _size
  def get(row: Int): Int                  = _data(row)
  def toArray(): Array[Int]               = doto(new Array[Int](size))(arr => System.arraycopy(_data, 0, arr, 0, size))
  def isEmpty: Boolean                    = size == 0
  def add(index: Int, element: Int): Unit = {
    checkRangeIncludingEndpoint(index)
    ensureCapacity(_size + 1)
    val numtomove = _size - index
    System.arraycopy(_data, index, _data, index+1, numtomove)
    _data(index) = element
    _size += 1
  }
  def add(element: Int): Boolean = {
    add(size(), element)
    true
  }
  private def checkRangeIncludingEndpoint(index: Int): Unit = {
    if (index < 0 || index > _size)
      throw new IndexOutOfBoundsException(s"Should be at least 0 and at most ${_size}, found $index")
  }
  def ensureCapacity(mincap: Int): Unit = {
    if (mincap > _data.length) {
      val newcap = (_data.length * 3) / 2 + 1
      val olddata = _data
      val newlen = math.max(mincap, newcap)
      _data = new Array[Int](newlen)
      System.arraycopy(olddata, 0, _data, 0, _size)
    }
  }
}

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
