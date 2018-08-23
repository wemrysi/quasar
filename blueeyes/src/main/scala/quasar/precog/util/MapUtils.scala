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

import scalaz.Either3

import scala.collection.generic.CanBuildFrom
import scala.{ collection => sc }

trait MapUtils {
  implicit def mapUtils[A, B, CC[B] <: sc.GenTraversable[B]](self: sc.GenMap[A, CC[B]]): MapExtras[A, B, CC] =
    new MapExtras(self)
}

class MapExtras[A, B, CC[B] <: sc.GenTraversable[B]](left: sc.GenMap[A, CC[B]]) {
  def cogroup[C, CC2[C] <: sc.GenTraversable[C], Result](right: sc.GenMap[A, CC2[C]])(
      implicit cbf: CanBuildFrom[Nothing, (A, Either3[B, (CC[B], CC2[C]), C]), Result]): Result = {
    val resultBuilder = cbf()

    left foreach {
      case (key, leftValues) => {
        right get key map { rightValues =>
          val _ =
            resultBuilder += (key -> Either3.middle3[B, (CC[B], CC2[C]), C]((leftValues, rightValues)))
        } getOrElse {
          leftValues foreach { b =>
            resultBuilder += (key -> Either3.left3[B, (CC[B], CC2[C]), C](b))
          }
        }
      }
    }

    right foreach {
      case (key, rightValues) => {
        if (!(left get key isDefined)) {
          rightValues foreach { c =>
            resultBuilder += (key -> Either3.right3[B, (CC[B], CC2[C]), C](c))
          }
        }
      }
    }

    resultBuilder.result()
  }
}
