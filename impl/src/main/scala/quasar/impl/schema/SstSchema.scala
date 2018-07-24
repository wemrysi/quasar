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

package quasar.impl.schema

import quasar.sst._

import scalaz.{\/, Equal, Show}
import scalaz.syntax.show._
import spire.algebra.{Field, NRoot}

final case class SstSchema[J, A](sst: SST[J, A] \/ PopulationSST[J, A])

object SstSchema {
  implicit def equal[J: Equal, A: Equal]: Equal[SstSchema[J, A]] =
    Equal.equalBy(
      _.sst.map(Population.unsubst[StructuralType[J, ?], TypeStat[A]]))

  implicit def show[J: Show, A: Equal: Field: NRoot: Show]: Show[SstSchema[J, A]] =
    Show.show(_.sst.fold(
      s => s,
      Population.unsubst[StructuralType[J, ?], TypeStat[A]]).show)
}
