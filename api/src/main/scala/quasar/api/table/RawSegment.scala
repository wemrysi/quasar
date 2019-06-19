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

import java.nio.charset.Charset
import java.util.Arrays

import cats.Eq
import cats.instances.either._
import cats.instances.int._
import cats.instances.list._
import cats.instances.string._
import cats.syntax.eq._

import scodec.{Attempt, Codec, Decoder, Err, SizeBound}
import scodec.bits._
import scodec.codecs._

/** An unparsed chunk of input from a structured source.
  *
  * @param context semantic location of the segment in the source
  * @param format the source data format
  * @param data the bytes comprising the segment
  */
@SuppressWarnings(Array("org.wartremover.warts.ArrayEquals"))
final case class RawSegment(context: StructurePath, format: SegmentFormat, data: Array[Byte])

object RawSegment {
  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  /** Equivalent to Codec#encode but attempts to avoid as much boxing as possible. */
  def encode(context: StructurePath, format: SegmentFormat, data: Array[Byte]): BitVector = {
    var count = 0
    var encodedCtx = BitVector.empty

    context foreach {
      case Left(i) =>
        count += 1
        encodedCtx ++= encodeArrNode(i)

      case Right(s) =>
        count += 1
        encodedCtx ++= encodeObjNode(s)
    }

    BitVector.fromInt(count) ++
    encodedCtx ++
    SegmentFormat.encode(format) ++
    BitVector.fromLong(data.length * 8L) ++
    BitVector.view(data)
  }

  implicit val binaryCodec: Codec[RawSegment] =
    new Codec[RawSegment] {
      private val nodeDecoder: Decoder[StructureNode] =
        ContextF.binaryCodec.asDecoder flatMap {
          case ContextF.Arr => int32.asDecoder.map(Left(_))
          case ContextF.Obj => utf8_32.asDecoder.map(Right(_))
        }

      private val contextDecoder: Decoder[StructurePath] =
        int32.asDecoder flatMap { count =>
          Decoder(Decoder.decodeCollect[List, StructureNode](nodeDecoder, Some(count))(_))
        }

      private val dataCodec: Codec[Array[Byte]] =
        variableSizeBitsLong(int64, bits.xmap(_.toByteArray, BitVector.view))

      private val decoder: Decoder[RawSegment] =
        for {
          context <- contextDecoder
          format <- SegmentFormat.binaryCodec.asDecoder
          data <- dataCodec.asDecoder
        } yield RawSegment(context, format, data)

      def decode(bv: BitVector) =
        decoder.decode(bv)

      def encode(c: RawSegment) =
        Attempt.successful(RawSegment.encode(c.context, c.format, c.data))

      val sizeBound =
        SizeBound(32L, None) +
        SegmentFormat.binaryCodec.sizeBound +
        SizeBound(64L, None)
    }

  implicit val eqv: Eq[RawSegment] =
    Eq instance {
      case (RawSegment(c1, f1, d1), RawSegment(c2, f2, d2)) =>
        c1 === c2 && f1 === f2 && Arrays.equals(d1, d2)
    }

  ////

  private sealed trait ContextF extends Product with Serializable

  private object ContextF {
    private val FlagBitWidth: Long = 4L

    case object Arr extends ContextF
    case object Obj extends ContextF

    val EncodedArr: BitVector = bin"0000"
    val EncodedObj: BitVector = bin"0001"

    implicit val binaryCodec: Codec[ContextF] =
      bits(FlagBitWidth).exmapc {
        case EncodedArr => Attempt.successful(Arr)
        case EncodedObj => Attempt.successful(Obj)
        case b => Attempt.failure(Err(s"Unrecognized context: $b"))
      } {
        case Arr => Attempt.successful(EncodedArr)
        case Obj => Attempt.successful(EncodedObj)
      }
  }

  private val utf8 = Charset.forName("UTF-8")

  private def encodeArrNode(index: Int): BitVector =
    ContextF.EncodedArr ++ BitVector.fromInt(index)

  private def encodeObjNode(field: String): BitVector = {
    val enc = BitVector.view(utf8.encode(field))
    ContextF.EncodedObj ++ BitVector.fromLong(enc.size / 8, 32) ++ enc
  }
}
