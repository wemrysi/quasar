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

import cats.data.NonEmptyList

import monocle.Prism

import quasar.api.table.ColumnType

import scala.Int

trait LegacyDestination[F[_]] extends UnparameterizedDestination[F] {

  type TypeId = ColumnType.Scalar

  val typeIdOrdinal: Prism[Int, ColumnType.Scalar] = {
    import ColumnType._

    Prism.partial[Int, ColumnType.Scalar] {
      case i if i == Null.ordinal => Null
      case i if i == Boolean.ordinal => Boolean
      case i if i == LocalTime.ordinal => LocalTime
      case i if i == OffsetTime.ordinal => OffsetTime
      case i if i == LocalDate.ordinal => LocalDate
      case i if i == OffsetDate.ordinal => OffsetDate
      case i if i == LocalDateTime.ordinal => LocalDateTime
      case i if i == OffsetDateTime.ordinal => OffsetDateTime
      case i if i == Interval.ordinal => Interval
      case i if i == Number.ordinal => Number
      case i if i == String.ordinal => String
    } (_.ordinal)
  }

  implicit val typeIdLabel: Label[ColumnType.Scalar] =
    Label.label[ColumnType.Scalar](_.toString)

  final def coerce(tpe: ColumnType.Scalar): TypeCoercion[Type] =
    TypeCoercion.Satisfied(NonEmptyList.one(tpe))
}
