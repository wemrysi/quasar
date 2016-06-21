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

package quasar.qscript

import quasar.Predef._

import simulacrum.typeclass

@typeclass trait Mergeable[A] {
  type IT[F[_]]

  def mergeSrcs(fm1: FreeMap[IT], fm2: FreeMap[IT], a1: A, a2: A):
      Option[Merge[IT, A]]
}

object Mergeable {
  type Aux[T[_[_]], A] = Mergeable[A] { type IT[F[_]] = T[F] }
}
