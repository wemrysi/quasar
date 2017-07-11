/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.contrib.matryoshka

import slamdata.Predef._

import scalaz._

sealed abstract class UWidth[F[_]] {
  val width: Int
}

object UWidth extends UWidthInstances

sealed abstract class UWidthInstances extends UWidthInstances0 {
  implicit def coproductUWidth[F[_], G[_]](
    implicit
    F: UWidth[F],
    G: UWidth[G]
  ): UWidth[Coproduct[F, G, ?]] =
    new UWidth[Coproduct[F, G, ?]] {
      val width = F.width + G.width
    }
}

sealed abstract class UWidthInstances0 {
  implicit def defaultUWidth[F[_]]: UWidth[F] =
    new UWidth[F] { val width = 1 }
}
