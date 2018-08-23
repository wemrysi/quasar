/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.precog.util

import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
  * Unchecked and unboxed (fast!) deque implementation with a fixed bound.  None
  * of the operations on this datastructure are checked for bounds.  You are
  * trusted to get this right on your own.  If you do something weird, you could
  * end up overwriting data, reading old results, etc.  Don't do that.
  *
  * No objects were allocated in the making of this film.
  */
final class RingDeque[@specialized(Boolean, Int, Long, Double, Float, Short) A: ClassTag](_bound: Int) {
  val bound = _bound + 1

  private val ring = new Array[A](bound)
  private var front = 0
  private var back  = rotate(front, 1)

  def isEmpty = front == rotate(back, -1)

  def empty() {
    back = rotate(front, 1)
  }

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

  def removeBack(length: Int) {
    moveBack(-length)
  }

  def length: Int =
    (if (back > front) back - front else (back + bound) - front) - 1

  def toList: List[A] = {
    @inline
    @tailrec
    def buildList(i: Int, accum: List[A]): List[A] =
      if (i < front)
        accum
      else
        buildList(i - 1, ring(i % bound) :: accum)

    buildList(front + length - 1, Nil)
  }

  @inline
  private[this] def rotate(target: Int, delta: Int) =
    (target + delta + bound) % bound

  @inline
  private[this] def moveFront(delta: Int) {
    front = rotate(front, delta)
  }

  @inline
  private[this] def moveBack(delta: Int) {
    back = rotate(back, delta)
  }
}
