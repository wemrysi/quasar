/*
 * Copyright 2020 Precog Data
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

package quasar.impl.storage

import scala.annotation.implicitNotFound

import shapeless._

@implicitNotFound("Could not prove that ${L} is a prefix of ${M}.")
sealed trait IsPrefix[L <: HList, M <: HList]

object IsPrefix {
  def apply[L <: HList, M <: HList](implicit isp: IsPrefix[L, M]): IsPrefix[L, M] = isp

  implicit def singletonIsPrefix[H, M <: HList]: IsPrefix[H :: HNil, H :: M] =
    new IsPrefix[H :: HNil, H :: M] {}

  implicit def prefixIsPrefix[H, L <: HList, M <: HList](
      implicit tailIsPrefix: IsPrefix[L, M])
      : IsPrefix[H :: L, H :: M] =
    new IsPrefix[H :: L, H :: M] {}
}
