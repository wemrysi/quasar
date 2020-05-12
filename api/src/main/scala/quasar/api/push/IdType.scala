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

package quasar.api.push

import slamdata.Predef.{Eq => _, _}

import quasar.api.ColumnType

import cats._

import monocle.Prism

sealed trait IdType extends Product with Serializable

object IdType {
  case object StringId extends IdType
  case object NumberId extends IdType

  val scalarP: Prism[ColumnType.Scalar, IdType] =
    Prism.partial[ColumnType.Scalar, IdType] {
      case ColumnType.String => StringId
      case ColumnType.Number => NumberId
    } {
      case StringId => ColumnType.String
      case NumberId => ColumnType.Number
    }

  implicit def idTypeEq: Eq[IdType] = Eq.fromUniversalEquals
  implicit def idTypeShow: Show[IdType] = Show.fromToString
}
