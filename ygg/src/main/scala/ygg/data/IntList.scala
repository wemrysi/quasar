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

package ygg.data

import ygg.common._

object ArrayIntList {
  val empty = new ArrayIntList(0)
}

class ArrayIntList(initialCapacity: Int) {
  private[this] var _size: Int        = 0
  private[this] var _data: Array[Int] = new Array[Int](initialCapacity)

  def intersect(bs: ArrayIntList): ArrayIntList = {
    val as = this

    //assertSorted(as)
    //assertSorted(bs)
    var i    = 0
    var j    = 0
    val alen = as.size
    val blen = bs.size
    val out  = new ArrayIntList(alen min blen)
    while (i < alen && j < blen) {
      val a = as.get(i)
      val b = bs.get(j)
      if (a < b) {
        i += 1
      } else if (a > b) {
        j += 1
      } else {
        out.add(a)
        i += 1
        j += 1
      }
    }
    out
  }

  def union(bs: ArrayIntList): ArrayIntList = {
    val as = this

    //assertSorted(as)
    //assertSorted(bs)
    var i    = 0
    var j    = 0
    val alen = as.size
    val blen = bs.size
    val out  = new ArrayIntList(alen max blen)
    while (i < alen && j < blen) {
      val a = as.get(i)
      val b = bs.get(j)
      if (a < b) {
        out.add(a)
        i += 1
      } else if (a > b) {
        out.add(b)
        j += 1
      } else {
        out.add(a)
        i += 1
        j += 1
      }
    }
    while (i < alen) {
      out.add(as.get(i))
      i += 1
    }
    while (j < blen) {
      out.add(bs.get(j))
      j += 1
    }
    out
  }

  def this()                              = this(8)
  def size(): Int                         = _size
  def get(row: Int): Int                  = _data(row)
  def toArray(): Array[Int]               = doto(new Array[Int](size))(arr => systemArraycopy(_data, 0, arr, 0, size))
  def isEmpty: Boolean                    = size == 0
  def add(index: Int, element: Int): Unit = {
    checkRangeIncludingEndpoint(index)
    ensureCapacity(_size + 1)
    val numtomove = _size - index
    systemArraycopy(_data, index, _data, index+1, numtomove)
    _data(index) = element
    _size += 1
  }
  def add(element: Int): Boolean = {
    add(size(), element)
    true
  }
  private def checkRangeIncludingEndpoint(index: Int): Unit = {
    if (index < 0 || index > _size)
      throw new java.lang.IndexOutOfBoundsException(s"Should be at least 0 and at most ${_size}, found $index")
  }
  def ensureCapacity(mincap: Int): Unit = {
    if (mincap > _data.length) {
      val newcap = (_data.length * 3) / 2 + 1
      val olddata = _data
      val newlen = scala.math.max(mincap, newcap)
      _data = new Array[Int](newlen)
      systemArraycopy(olddata, 0, _data, 0, _size)
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
