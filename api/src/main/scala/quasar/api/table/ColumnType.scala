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

import slamdata.Predef.{Int, Product, Serializable}

import scalaz.{Order, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

sealed trait ColumnType extends Product with Serializable

object ColumnType {
  sealed trait Scalar extends ColumnType
  final case object Null extends Scalar
  final case object Boolean extends Scalar
  final case object Number extends Scalar
  final case object OffsetDateTime extends Scalar
  final case object String extends Scalar

  sealed trait Vector extends ColumnType
  final case object Array extends Vector
  final case object Object extends Vector

  implicit def columnTypeOrder[T <: ColumnType]: Order[T] =
    Order.order((x, y) => asInt(x) ?|? asInt(y))

  implicit def columnTypeShow[T <: ColumnType]: Show[T] =
    Show.showFromToString

  ////

  private val asInt: ColumnType => Int = {
    case Null => 0
    case Boolean => 1
    case Number => 2
    case OffsetDateTime => 3
    case String => 4
    case Array => 5
    case Object => 6
  }
}
