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

import java.util.concurrent.ConcurrentHashMap

final class LazyMap[A, B, C](source: Map[A, B], f: B => C) extends Map[A, C] {
  private val m = new ConcurrentHashMap[A, C]()

  def iterator: Iterator[(A, C)] = source.keysIterator map (a => a -> apply(a))

  def get(a: A): Option[C] = m get a match {
    case null =>
      source get a map { b =>
        val c = f(b)
        m.putIfAbsent(a, c)
        c
      }
    case x => Some(x)
  }
  def +[C1 >: C](kv: (A, C1)): Map[A, C1] = iterator.toMap + kv
  def -(a: A): Map[A, C]                  = iterator.toMap - a
}
