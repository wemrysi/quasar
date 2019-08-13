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

package quasar.connector

import slamdata.Predef._

import tectonic.csv.Parser.Config

import monocle.Prism
import scalaz.{Enum, Equal, Show}
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.tuple._
//import scalaz.syntax.equal._
import scalaz.syntax.order._

import argonaut.{DecodeResult, CodecJson, Argonaut}, Argonaut._

sealed trait ParsableType extends Product with Serializable

object ParsableType extends ParsableTypeInstances {
  final case class Json(variant: JsonVariant, isPrecise: Boolean) extends ParsableType
  final case class SeparatedValues(config: Config) extends ParsableType

  sealed trait JsonVariant extends Product with Serializable

  object JsonVariant {
    case object ArrayWrapped extends JsonVariant
    case object LineDelimited extends JsonVariant

    implicit val jsonVariantCodecJson: CodecJson[JsonVariant] = CodecJson({
      case ArrayWrapped => "array-wrapped".asJson
      case LineDelimited => "line-delimited".asJson
    }, (c => c.as[String].flatMap {
      case "array-wrapped" => DecodeResult.ok(ArrayWrapped)
      case "line-delimited" => DecodeResult.ok(LineDelimited)
    }))

    implicit val enum: Enum[JsonVariant] =
      new Enum[JsonVariant] {
        def order(x: JsonVariant, y: JsonVariant) =
          toInt(x) ?|? toInt(y)

        def pred(jv: JsonVariant): JsonVariant =
          jv match {
            case ArrayWrapped => LineDelimited
            case LineDelimited => ArrayWrapped
          }

        def succ(jv: JsonVariant): JsonVariant =
          jv match {
            case ArrayWrapped => LineDelimited
            case LineDelimited => ArrayWrapped
          }

        def toInt(jv: JsonVariant): Int =
          jv match {
            case ArrayWrapped => 0
            case LineDelimited => 1
          }
      }

    implicit val show: Show[JsonVariant] =
      Show.showFromToString
  }

  implicit val parsableTypeCodecJson: CodecJson[ParsableType] = {
    CodecJson({
      case Json(v, precise) =>
        ("type" := "json") ->:
        ("variant" := v) ->:
        ("precise" := precise) ->:
        jEmptyObject
      case SeparatedValues(config) =>
        ("type" := "separated-values") ->:
        ("header" := config.header) ->:
        ("row1" := config.row1.toChar) ->:
        ("row2" := (if (config.row2 === 0.toByte) "" else config.row2.toChar.toString)) ->:
        ("record" := config.record.toChar) ->:
        ("openQuote" := config.openQuote.toChar) ->:
        ("closeQuote" := config.closeQuote.toChar) ->:
        ("escape" := config.escape.toChar) ->:
        jEmptyObject
    }, (c => (c --\ "type").as[String].flatMap {
      case "json" =>
        for {
          variant <- (c --\ "variant").as[JsonVariant]
          isPrecise <- (c --\ "precise").as[Option[Boolean]]
        } yield Json(variant, isPrecise.getOrElse(false))
      case "separated-values" =>
        for {
          header <- (c --\ "header").as[Boolean]
          row1 <- (c --\ "row1").as[Char]
          row2 <- (c --\ "row2").as[String]
          record <- (c --\ "record").as[Char]
          openQuote <- (c --\ "openQuote").as[Char]
          closeQuote <- (c --\ "closeQuote").as[Char]
          escape <- (c --\ "escape").as[Char]
        } yield SeparatedValues(Config(
          header = header,
          row1 = row1.toByte,
          row2 = row2.headOption match {
            case None => 0.toByte
            case Some(h) => h.toByte
          },
          record = record.toByte,
          openQuote = openQuote.toByte,
          closeQuote = closeQuote.toByte,
          escape = escape.toByte
        ))
      case _ => DecodeResult.fail("tololo", c.history)
    }))
  }

  val json: Prism[ParsableType, (JsonVariant, Boolean)] =
    Prism.partial[ParsableType, (JsonVariant, Boolean)] {
      case Json(v, p) => (v, p)
    } ((Json(_, _)).tupled)

  val separatedValues: Prism[ParsableType, Config] =
    Prism.partial[ParsableType, Config] {
      case SeparatedValues(c) => c
    } (SeparatedValues(_))
}

sealed abstract class ParsableTypeInstances {
  implicit val equal: Equal[ParsableType] =
    Equal.equalBy(ParsableType.json.getOption(_))

  implicit val show: Show[ParsableType] =
    Show.showFromToString
}
