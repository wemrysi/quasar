package quasar

import blueeyes._
import scalaz._, Scalaz._, Ordering._

package ygg {
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
}

package object ygg {
  type Identity   = Long
  type Identities = Array[Identity]

  def prefixIdentityOrdering(ids1: Identities, ids2: Identities, prefixLength: Int): Cmp = {
    var i = 0
    while (i < prefixLength) {
      longInstance.order(ids1(i), ids2(i)) match {
        case EQ  => i += 1
        case cmp => return cmp
      }
    }
    EQ
  }

  object IdentitiesOrder extends Ord[Identities] {
    def order(ids1: Identities, ids2: Identities): Cmp = fullIdentityOrdering(ids1, ids2)
  }

  def fullIdentityOrdering(ids1: Identities, ids2: Identities): Cmp =
    prefixIdentityOrdering(ids1, ids2, ids1.length min ids2.length)

  def prefixIdentityOrder(prefixLength: Int): Ord[Identities] =
    Ord order ((ids1, ids2) => prefixIdentityOrdering(ids1, ids2, prefixLength))

  def identityValueOrder[A](idOrder: Ord[Identities])(implicit ord: Ord[A]): Ord[Identities -> A] =
    Ord order ((x, y) => idOrder.order(x._1, y._1) |+| (x._2 ?|? y._2))

  def tupledIdentitiesOrder[A](ord: Ord[Identities]): Ord[Identities -> A] = ord contramap (_._1)
  def valueOrder[A](ord: Ord[A]): Ord[Identities -> A]                     = ord contramap (_._2)
}
