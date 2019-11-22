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

import scala.util.Right

trait LegacyDestination[F[_]] extends UnparameterizedDestination[F] {
  type Type = ColumnType.Scalar

  implicit val labelType: Label[ColumnType.Scalar] =
    Label.label[ColumnType.Scalar](_.toString)

  implicit val eqType: Eq[ColumnType.Scalar] =
    Eq[ColumnType.Scalar]

  implicit val codecJsonType: CodecJson[ColumnType.Scalar] =
    CodecJson.derived[ColumnType.Scalar]

  final def coerce(tpe: ColumnType.Scalar): TypeCoercion[Constructor, Type] =
    TypeCoercion.Satisfied(NonEmptyList.one(Right(tpe)))
}
