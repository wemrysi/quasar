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

package quasar.api.table

import argonaut._, Argonaut._

import slamdata.Predef.{Int, Product, Serializable, Set}

import cats.{Order, Show}
import cats.implicits._

import scala.StringContext

sealed abstract class ColumnType(final val ordinal: Int) extends Product with Serializable

private[table] trait LowPriorityImplicits {
  import ColumnType._

  implicit val codecJson: CodecJson[ColumnType] =
    CodecJson(
      {
        case Null => "null".asJson
        case Boolean => "boolean".asJson
        case LocalTime => "localtime".asJson
        case OffsetTime => "offsettime".asJson
        case LocalDate => "localdate".asJson
        case OffsetDate => "offsetdate".asJson
        case LocalDateTime => "localdatetime".asJson
        case OffsetDateTime => "offsetdatetime".asJson
        case Interval => "interval".asJson
        case Number => "number".asJson
        case String => "string".asJson
        case Array => "array".asJson
        case Object => "object".asJson
      },
      { c =>
        c.as[java.lang.String] flatMap {
          case "null" => DecodeResult.ok(Null)
          case "boolean" => DecodeResult.ok(Boolean)
          case "localtime" => DecodeResult.ok(LocalTime)
          case "offsettime" => DecodeResult.ok(OffsetTime)
          case "localdate" => DecodeResult.ok(LocalDate)
          case "offsetdate" => DecodeResult.ok(OffsetDate)
          case "localdatetime" => DecodeResult.ok(LocalDateTime)
          case "offsetdatetime" => DecodeResult.ok(OffsetDateTime)
          case "interval" => DecodeResult.ok(Interval)
          case "number" => DecodeResult.ok(Number)
          case "string" => DecodeResult.ok(String)
          case "array" => DecodeResult.ok(Array)
          case "object" => DecodeResult.ok(Object)
          case s => DecodeResult.fail(s"unknown column type '$s'", c.history)
        }
      })
}

object ColumnType extends LowPriorityImplicits {

  sealed abstract class Scalar(ordinal: Int) extends ColumnType(ordinal)

  case object Null extends Scalar(0)
  case object Boolean extends Scalar(1)
  case object LocalTime extends Scalar(2)
  case object OffsetTime extends Scalar(3)
  case object LocalDate extends Scalar(4)
  case object OffsetDate extends Scalar(5)
  case object LocalDateTime extends Scalar(6)
  case object OffsetDateTime extends Scalar(7)
  case object Interval extends Scalar(8)
  case object Number extends Scalar(9)
  case object String extends Scalar(10)

  sealed abstract class Vector(ordinal: Int) extends ColumnType(ordinal)

  case object Array extends Vector(11)
  case object Object extends Vector(12)

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
    Order.by(_.ordinal)

  implicit def columnTypeShow[T <: ColumnType]: Show[T] =
    Show.fromToString

  implicit val codecJsonScalar: CodecJson[ColumnType.Scalar] =
    CodecJson(
      {
        case Null => "null".asJson
        case Boolean => "boolean".asJson
        case LocalTime => "localtime".asJson
        case OffsetTime => "offsettime".asJson
        case LocalDate => "localdate".asJson
        case OffsetDate => "offsetdate".asJson
        case LocalDateTime => "localdatetime".asJson
        case OffsetDateTime => "offsetdatetime".asJson
        case Interval => "interval".asJson
        case Number => "number".asJson
        case String => "string".asJson
      },
      { c =>
        c.as[java.lang.String] flatMap {
          case "null" => DecodeResult.ok(Null)
          case "boolean" => DecodeResult.ok(Boolean)
          case "localtime" => DecodeResult.ok(LocalTime)
          case "offsettime" => DecodeResult.ok(OffsetTime)
          case "localdate" => DecodeResult.ok(LocalDate)
          case "offsetdate" => DecodeResult.ok(OffsetDate)
          case "localdatetime" => DecodeResult.ok(LocalDateTime)
          case "offsetdatetime" => DecodeResult.ok(OffsetDateTime)
          case "interval" => DecodeResult.ok(Interval)
          case "number" => DecodeResult.ok(Number)
          case "string" => DecodeResult.ok(String)
          case s => DecodeResult.fail(s"unknown column type '$s'", c.history)
        }
      })
}
