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

import slamdata.Predef._

import cats.Eq

import scodec.{Attempt, Codec, Decoder, Err, SizeBound}
import scodec.bits._
import scodec.codecs._

sealed trait SegmentFormat extends Product with Serializable

object SegmentFormat {
  private val FlagBitWidth: Long = 4L
  private val JsonF = bin"0000"
  private val CsvF = bin"0001"

  case object Json extends SegmentFormat

  final case class Csv(record: Byte, openQuote: Byte, closeQuote: Byte, escape: Byte)
    extends SegmentFormat

  /** Equivalent to Codec#encode but attempts to avoid as much boxing as possible. */
  def encode(fmt: SegmentFormat): BitVector =
    fmt match {
      case Json => encodeJson
      case Csv(r, o, c, e) => encodeCsv(r, o, c, e)
    }

  val encodeJson: BitVector =
    JsonF

  def encodeCsv(record: Byte, openQuote: Byte, closeQuote: Byte, escape: Byte): BitVector =
    CsvF ++
    BitVector.fromByte(record) ++
    BitVector.fromByte(openQuote) ++
    BitVector.fromByte(closeQuote) ++
    BitVector.fromByte(escape)

  implicit val binaryCodec: Codec[SegmentFormat] =
    new Codec[SegmentFormat] {
      val csvDec =
        (byte ~ byte ~ byte ~ byte).asDecoder.map(Csv)

      val dec = bits(FlagBitWidth).asDecoder flatMap {
        case JsonF => Decoder.point(Json)
        case CsvF => csvDec
        case b => Decoder(_ => Attempt.failure(Err(s"Unrecognized format flag: $b")))
      }

      def decode(bv: BitVector) = dec.decode(bv)

      def encode(fmt: SegmentFormat) = Attempt.successful(SegmentFormat.encode(fmt))

      val sizeBound = SizeBound(FlagBitWidth, Some(FlagBitWidth + 32L))
    }

  implicit val eqv: Eq[SegmentFormat] =
    Eq.fromUniversalEquals
}
