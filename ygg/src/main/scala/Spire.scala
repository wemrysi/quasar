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

package ygg.external

import ygg.common._
import scalaz._, Scalaz._

/**
  * In-place merge sort implementation. This sort is stable but does mutate
  * the given array. It is an in-place sort but it does allocate a temporary
  * array of the same size as the input. It uses InsertionSort for sorting very
  * small arrays.
  */
object MergeSort {
  @inline final def startWidth: Int = 8
  @inline final def startStep: Int  = 16

  final def sort[@spec A: Ord: CTag](data: Array[A]): Unit = {
    val len = data.length

    if (len <= startStep) return InsertionSort.sort(data)

    var buf1: Array[A] = data
    var buf2: Array[A] = new Array[A](len)
    var tmp: Array[A]  = null

    var i     = 0
    var limit = len - startWidth
    while (i < limit) { InsertionSort.sort(data, i, i + startWidth); i += startWidth }
    if (i < len) InsertionSort.sort(data, i, len)
    var width = startWidth
    var step  = startStep
    while (width < len) {
      i = 0
      limit = len - step
      while (i < limit) {
        merge(buf1, buf2, i, i + width, i + step); i += step
      }
      while (i < len) {
        merge(buf1, buf2, i, scala.math.min(i + width, len), len); i += step
      }
      tmp = buf2
      buf2 = buf1
      buf1 = tmp

      width *= 2
      step *= 2
    }

    if (buf1 != data) systemArraycopy(buf1, 0, data, 0, len)
  }

  /**
    * Helper method for mergeSort, used to do a single "merge" between two
    * sections of the input array. The start, mid and end parameters denote the
    * left and right ranges of the input to merge, as well as the area of the
    * ouput to write to.
    */
  @inline final def merge[@spec A: Ord](in: Array[A], out: Array[A], start: Int, mid: Int, end: Int): Unit = {
    var ii = start
    var jj = mid
    var kk = start
    while (kk < end) {
      if (ii < mid && (jj >= end || in(ii) <= in(jj))) {
        out(kk) = in(ii); ii += 1
      } else {
        out(kk) = in(jj); jj += 1
      }
      kk += 1
    }
  }
}

object InsertionSort {
  final def sort[@spec A: Ord: CTag](data: Array[A]): Unit = sort(data, 0, data.length)
  final def sort[@spec A: Ord: CTag](data: Array[A], start: Int, end: Int): Unit = {
    var i = start + 1
    while (i < end) {
      val item = data(i)
      var hole = i
      while (hole > start && data(hole - 1) > item) {
        data(hole) = data(hole - 1)
        hole -= 1
      }
      data(hole) = item
      i += 1
    }
  }
}
