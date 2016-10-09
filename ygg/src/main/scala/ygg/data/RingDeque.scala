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

/**
  * Unchecked and unboxed (fast!) deque implementation with a fixed bound.  None
  * of the operations on this datastructure are checked for bounds.  You are
  * trusted to get this right on your own.  If you do something weird, you could
  * end up overwriting data, reading old results, etc.  Don't do that.
  *
  * No objects were allocated in the making of this film.
  */
final class RingDeque[@spec(Boolean, Int, Long, Double) A: CTag](_bound: Int) {
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

  def pushFront(a: A): Unit = {
    moveFront(-1)
    ring(front) = a
  }

  def popBack(): A = {
    moveBack(-1)
    ring(rotate(back, -1))
  }

  def pushBack(a: A): Unit = {
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
