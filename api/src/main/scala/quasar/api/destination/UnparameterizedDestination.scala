/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.api.destination

import argonaut.CodecJson

import cats.Eq

import quasar.contrib.std.errorImpossible
import quasar.fp.Dependent

import scala.Nothing

import skolems.∃

/** A destination without any type constructors. */
trait UnparameterizedDestination[F[_]] extends Destination[F] {
  type Constructor[A] = Nothing

  implicit val labelConstructor: Label[∃[Constructor]] =
    Label.label[∃[Constructor]](_ => errorImpossible)

  implicit val eqConstructor: Eq[∃[Constructor]] =
    Eq.allEqual[∃[Constructor]]

  implicit val codecJsonConstructor: CodecJson[∃[Constructor]] =
    CodecJson[∃[Constructor]](_ => errorImpossible, _ => errorImpossible)

  implicit val dependentEq: Dependent[Constructor, Eq] =
    λ[Dependent[Constructor, Eq]](_ => errorImpossible)

  implicit val dependentCodecJson: Dependent[Constructor, CodecJson] =
    λ[Dependent[Constructor, CodecJson]](_ => errorImpossible)
}
