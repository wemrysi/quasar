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
import argonaut.Argonaut.{UnitDecodeJson, UnitEncodeJson}

import cats.Eq
import cats.data.NonEmptyList

import quasar.api.table.ColumnType
import quasar.contrib.std.errorImpossible
import quasar.fp.Dependent

import scala.{sys, Nothing, Unit}
import scala.util.Right

trait UntypedDestination[F[_]] extends Destination[F] {
  type Type = Unit
  type Constructor[A] = Nothing

  implicit val labelType: Label[Unit] = Label.label[Unit](_ => "()")

  implicit val eqType: Eq[Unit] = Eq[Unit]

  implicit val jsonCodecType: CodecJson[Unit] =
    CodecJson[Unit](UnitEncodeJson.encode, UnitDecodeJson.decode)

  implicit def labelConstructor[P]: Label[Nothing] =
    Label.label[Nothing](_ => errorImpossible)

  implicit def eqConstructor[P]: Eq[Nothing] =
    Eq.by[Nothing, Nothing](_ => errorImpossible)

  implicit def jsonCodecConstructor[P]: CodecJson[Nothing] =
    CodecJson[Nothing](_ => errorImpossible, _ => errorImpossible)

  implicit val dependentLabel: Dependent[Constructor, Label] =
    λ[Dependent[Constructor, Label]](_ => errorImpossible)

  implicit val dependentEq: Dependent[Constructor, Eq] =
    λ[Dependent[Constructor, Eq]](_ => errorImpossible)

  implicit val dependentCodecJson: Dependent[Constructor, CodecJson] =
    λ[Dependent[Constructor, CodecJson]](_ => errorImpossible)

  final def coerce(tpe: ColumnType): TypeCoercion[Constructor, Type] =
    TypeCoercion.Satisfied(NonEmptyList.one(Right(())))
}
