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

package quasar.blueeyes

import scala.annotation.tailrec

/**
  * This object contains some methods to do faster iteration over primitives.
  *
  * In particular it doesn't box, allocate intermediate objects, or use a (slow)
  * shared interface with scala collections.
  */
object Loop {
  @tailrec
  def range(i: Int, limit: Int)(f: Int => Unit) {
    if (i < limit) {
      f(i)
      range(i + 1, limit)(f)
    }
  }

  final def forall[@specialized A](as: Array[A])(f: A => Boolean): Boolean = {
    @tailrec def loop(i: Int): Boolean = i == as.length || f(as(i)) && loop(i + 1)

    loop(0)
  }
}
