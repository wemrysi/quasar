/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package blueeyes

object BitSetUtil {
  def fromArray(arr: Array[Long]) = {
    val bs = new BitSet()
    bs.setBits(arr)
    bs
  }

  def create(): BitSet = new BitSet()

  def create(ns: Array[Int]): BitSet = {
    val bs  = new BitSet()
    var i   = 0
    val len = ns.length
    while (i < len) {
      bs.set(ns(i))
      i += 1
    }
    bs
  }

  def create(ns: Seq[Int]): BitSet = {
    val bs = new BitSet()
    ns.foreach(n => bs.set(n))
    bs
  }

  def range(start: Int, end: Int): BitSet = {
    val bs = new BitSet()
    Loop.range(start, end)(i => bs.set(i))
    bs
  }

  def takeRange(from: Int, to: Int)(bitset: BitSet): BitSet = {
    val len = bitset.length
    if (from <= 0) {
      val bits = bitset.copy()
      if (to >= len) bits
      else {
        bits.clear(to, len)
        bits
      }
    } else {
      var i    = from
      val bits = new BitSet()
      while (i < to) {
        bits(i - from) = bitset(i)
        i += 1
      }
      bits
    }
  }

  def filteredRange(start: Int, end: Int)(pred: Int => Boolean): BitSet = {
    val bs = new BitSet()
    Loop.range(start, end)(i => if (pred(i)) bs.set(i))
    bs
  }

  @inline final def filteredRange(r: Range)(pred: Int => Boolean): BitSet =
    filteredRange(r.start, r.end)(pred)
}
