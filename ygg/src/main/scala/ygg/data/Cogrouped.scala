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
import scalaz._, Either3._

object Cogrouped {
  type Element[V, V1, CC[X]]   = Either3[V, CC[V] -> CC[V1], V1]
  type Result[K, V, V1, CC[X]] = Seq[K -> Element[V, V1, CC]]

  final case class Builder[K, V, V1, CC[X] <: scIterable[X]](lmap: scMap[K, CC[V]], rmap: scMap[K, CC[V1]]) {
    lazy val leftKeys   = lmap.keySet -- rmap.keySet
    lazy val middleKeys = lmap.keySet & rmap.keySet
    lazy val rightKeys  = rmap.keySet -- lmap.keySet

    def build[That](implicit cbf: CBF[_, K -> Either3[V, CC[V] -> CC[V1], V1], That]): That = {
      val buf = cbf()

      for (k <- leftKeys ; v <- lmap(k)) buf += (k -> left3(v))
      for (k <- rightKeys ; v <- rmap(k)) buf += (k -> right3(v))
      for (k <- middleKeys) buf += (k -> middle3(lmap(k) -> rmap(k)))

      buf.result()
    }
  }
}
