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

package quasar.impl.parsing

import slamdata.Predef._

import quasar.ScalarStages
import quasar.connector.{CompressionScheme, QueryResult, DataFormat}, DataFormat.JsonVariant

import java.lang.IllegalArgumentException

import scala.collection.mutable.ArrayBuffer

import cats.effect.Sync

import fs2.{gzip, Chunk, Stream, Pipe}

import qdata.{QData, QDataEncode}
import qdata.tectonic.QDataPlate

import scalaz.syntax.equal._

import tectonic.fs2.StreamParser
import tectonic.{json, csv}

object ResultParser {
  val DefaultDecompressionBufferSize: Int = 32768

  private def concatArrayBufs[A](bufs: List[ArrayBuffer[A]]): ArrayBuffer[A] = {
    val totalSize = bufs.foldLeft(0)(_ + _.length)
    bufs.foldLeft(new ArrayBuffer[A](totalSize))(_ ++= _)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def typed[F[_]: Sync, A: QDataEncode](format: DataFormat): Pipe[F, Byte, A] = {
    format match {
      case DataFormat.Json(vnt, isPrecise) =>
        val mode: json.Parser.Mode = vnt match {
          case JsonVariant.ArrayWrapped => json.Parser.UnwrapArray
          case JsonVariant.LineDelimited => json.Parser.ValueStream
        }
        StreamParser(json.Parser(QDataPlate[F, A, ArrayBuffer[A]](isPrecise), mode))(
          Chunk.buffer,
          bufs => Chunk.buffer(concatArrayBufs[A](bufs)))
      case sv: DataFormat.SeparatedValues =>
        val config = csv.Parser.Config(
          header = sv.header,
          row1 = sv.row1.toByte,
          row2 = sv.row2.toByte,
          record = sv.record.toByte,
          openQuote = sv.openQuote.toByte,
          closeQuote = sv.closeQuote.toByte,
          escape = sv.escape.toByte
        )
        StreamParser(csv.Parser(QDataPlate[F, A, ArrayBuffer[A]](false), config))(
          Chunk.buffer,
          bufs => Chunk.buffer(concatArrayBufs[A](bufs)))
      case DataFormat.Compressed(CompressionScheme.Gzip, pt) =>
        gzip.decompress[F](DefaultDecompressionBufferSize) andThen typed[F, A](pt)
    }
  }

  def apply[F[_]: Sync, A: QDataEncode](queryResult: QueryResult[F]): Stream[F, A] = {
    def parsedStream(qr: QueryResult[F]): Stream[F, A] =
      qr match {
        case QueryResult.Parsed(qdd, data, _) =>
          data.map(QData.convert(_)(qdd, QDataEncode[A]))

        case QueryResult.Typed(pt, data, _) =>
          data.through(typed[F, A](pt))

      }
    if (queryResult.stages === ScalarStages.Id)
      parsedStream(queryResult)
    else
      // TODO: Be nice to have a static representation of the absence of parse instructions
      Stream.raiseError(new IllegalArgumentException("ParseInstructions not supported."))
  }
}
