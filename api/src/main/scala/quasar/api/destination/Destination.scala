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
import cats.data.NonEmptyList

import quasar.api.table.ColumnType
import quasar.fp.Dependent

import skolems.∃

/**
 * @see quasar.api.destination.UntypedDestination
 */
trait Destination[F[_]] {
  type Type

  implicit val labelType: Label[Type]
  implicit val eqType: Eq[Type]
  implicit val jsonCodecType: CodecJson[Type]

  type Constructor[P] <: ConstructorLike[P]

  implicit val labelConstructor: Label[∃[Constructor]]
  implicit val eqConstructor: Eq[∃[Constructor]]
  implicit val codecJsonConstructor: CodecJson[∃[Constructor]]

  implicit val dependentEq: Dependent[Constructor, Eq]
  implicit val dependentCodecJson: Dependent[Constructor, CodecJson]

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[Constructor, Type]

  def destinationType: DestinationType

  def sinks: NonEmptyList[ResultSink[F, Type]]

  trait ConstructorLike[P] { this: Constructor[P] =>
    def apply(p: P): Type
  }
}
