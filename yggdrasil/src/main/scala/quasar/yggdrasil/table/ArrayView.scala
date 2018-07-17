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

package quasar
package yggdrasil
package table

import quasar.precog.common.RValue

// if we use this with primitives ever, specialize it.
private[table] final case class ArraySliced[A](arr: Array[A], start: Int, size: Int) {
  def head: A = arr(start)
  def tail: ArraySliced[A] = ArraySliced(arr, start + 1, size - 1)
}

private[table] object ArraySliced {
  val noRValues = ArraySliced(new Array[RValue](0), 0, 0)
}

