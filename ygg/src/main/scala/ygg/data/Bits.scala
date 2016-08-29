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
