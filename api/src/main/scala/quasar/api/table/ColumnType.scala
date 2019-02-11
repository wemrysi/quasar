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

package quasar.api.table

import slamdata.Predef.{Int, Product, Serializable, Set}

import scalaz.{Order, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

sealed trait ColumnType extends Product with Serializable

object ColumnType {
  sealed trait Scalar extends ColumnType
  final case object Null extends Scalar
  final case object Boolean extends Scalar
  final case object LocalTime extends Scalar
  final case object OffsetTime extends Scalar
  final case object LocalDate extends Scalar
  final case object OffsetDate extends Scalar
  final case object LocalDateTime extends Scalar
  final case object OffsetDateTime extends Scalar
  final case object Interval extends Scalar
  final case object Number extends Scalar
  final case object String extends Scalar

  sealed trait Vector extends ColumnType
  final case object Array extends Vector
  final case object Object extends Vector

  val Top: Set[ColumnType] =
    Set(
      Null,
      Boolean,
      LocalTime,
      OffsetTime,
      LocalDate,
      OffsetDate,
      LocalDateTime,
      OffsetDateTime,
      Interval,
      Number,
      String,
      Array,
      Object)

  implicit def columnTypeOrder[T <: ColumnType]: Order[T] =
    Order.order((x, y) => asInt(x) ?|? asInt(y))

  implicit def columnTypeShow[T <: ColumnType]: Show[T] =
    Show.showFromToString

  ////

  private val asInt: ColumnType => Int = {
    case Null => 0
    case Boolean => 1
    case LocalTime => 2
    case OffsetTime => 3
    case LocalDate => 4
    case OffsetDate => 5
    case LocalDateTime => 6
    case OffsetDateTime => 7
    case Interval => 8
    case Number => 9
    case String => 10
    case Array => 11
    case Object => 12
  }
}
