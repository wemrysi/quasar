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

package quasar.impl.parsing

import slamdata.Predef._

import quasar.ScalarStages
import quasar.{concurrent => qc}
import quasar.connector.{CompressionScheme, QueryResult, DataFormat}, DataFormat.JsonVariant

import java.lang.IllegalArgumentException
import java.util.zip.ZipInputStream

import scala.collection.mutable.ArrayBuffer

import cats.data.OptionT
import cats.effect.{Sync, ConcurrentEffect, ContextShift}
import cats.effect.Blocker
import cats.implicits._

import fs2.{Chunk, Stream, Pipe}
import fs2.compression
import fs2.io

import qdata.{QData, QDataEncode}
import qdata.tectonic.QDataPlate

import scalaz.syntax.equal._

import tectonic.{json, csv, MultiplexingPlate, Plate}
import tectonic.fs2.StreamParser

object ResultParser {
  val DefaultDecompressionBufferSize: Int = 32768
  val blocker = qc.Blocker.cached("sdbe-runner")

  private def concatArrayBufs[A](bufs: List[ArrayBuffer[A]]): ArrayBuffer[A] = {
    val totalSize = bufs.foldLeft(0)(_ + _.length)
    bufs.foldLeft(new ArrayBuffer[A](totalSize))(_ ++= _)
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def typed[F[_]: Sync: ConcurrentEffect: ContextShift, A: QDataEncode](format: DataFormat): Pipe[F, Byte, A] = {
    format match {
      case DataFormat.Json(vnt, isPrecise) =>
        val mode: json.Parser.Mode = vnt match {
          case JsonVariant.ArrayWrapped => json.Parser.UnwrapArray
          case JsonVariant.LineDelimited => json.Parser.ValueStream
        }

        StreamParser(json.Parser(QDataPlate[F, A, ArrayBuffer[A]](isPrecise), mode))(Chunk.buffer)

      case sv: DataFormat.SeparatedValues =>
        val config = csv.Parser.Config(
          header = sv.header,
          row1 = sv.row1.toByte,
          row2 = sv.row2.toByte,
          record = sv.record.toByte,
          openQuote = sv.openQuote.toByte,
          closeQuote = sv.closeQuote.toByte,
          escape = sv.escape.toByte)

        StreamParser(csv.Parser(QDataPlate[F, A, ArrayBuffer[A]](false), config))(Chunk.buffer)

      case DataFormat.Compressed(CompressionScheme.Gzip, pt) =>
        compression
          .gunzip[F](DefaultDecompressionBufferSize)
          .andThen(_.flatMap(_.content))
          .andThen(typed[F, A](pt))

      case DataFormat.Compressed(CompressionScheme.Zip, pt) => {
        unzip[F](_, blocker, DefaultDecompressionBufferSize).flatMap(t => t._2)
      }
    }
  }

  def apply[F[_]: Sync: ConcurrentEffect: ContextShift, A: QDataEncode](queryResult: QueryResult[F]): Stream[F, A] = {
    def parsedStream(qr: QueryResult[F]): Stream[F, A] =
      qr match {
        case QueryResult.Parsed(qdd, data, _) =>
          data.map(QData.convert(_)(qdd, QDataEncode[A]))

        case QueryResult.Typed(pt, data, _) =>
          data.through(typed[F, A](pt))

        case QueryResult.Stateful(format, plateF, state, data, stages) =>
          Stream.eval(plateF) flatMap { plate =>
            val pipe = stateful(format, plate, state, data)

            data(None).through(pipe) ++
              recurseStateful(state(plate), data, pipe)
          }
      }

    if (queryResult.stages === ScalarStages.Id)
      parsedStream(queryResult)
    else
      // TODO: Be nice to have a static representation of the absence of parse instructions
      Stream.raiseError[F](new IllegalArgumentException("ParseInstructions not supported."))
  }

  private def recurseStateful[F[_]: Sync, P <: Plate[Unit], S, A](
      state: F[Option[S]],
      data: Option[S] => Stream[F, Byte],
      pipe: Pipe[F, Byte, A])
      : Stream[F, A] =
    Stream.eval(state) flatMap {
      case s @ Some(_) =>
        data(s).through(pipe) ++
          recurseStateful(state, data, pipe)
      case None =>
        Stream.empty
    }

  private def stateful[F[_]: Sync, P <: Plate[Unit], S, A: QDataEncode](
      format: DataFormat,
      plate: P,
      state: P => F[Option[S]],
      data: Option[S] => Stream[F, Byte])
      : Pipe[F, Byte, A] =
    format match {
      case DataFormat.Compressed(CompressionScheme.Gzip, pt) =>
        compression
          .gunzip[F](DefaultDecompressionBufferSize)
          .andThen(_.flatMap(_.content))
          .andThen(stateful(pt, plate, state, data))

      case DataFormat.Compressed(CompressionScheme.Zip, pt) => {
        println("using that inflate")
        compression
          .inflate[F](false, DefaultDecompressionBufferSize)
          //.andThen(_.flatMap(_.content))
          .andThen(stateful(pt, plate, state, data))
      }

      case DataFormat.Json(vnt, isPrecise) =>
        val mode: json.Parser.Mode = vnt match {
          case JsonVariant.ArrayWrapped => json.Parser.UnwrapArray
          case JsonVariant.LineDelimited => json.Parser.ValueStream
        }

        val parserPlate: F[Plate[ArrayBuffer[A]]] =
          QDataPlate[F, A, ArrayBuffer[A]](isPrecise).map(
            MultiplexingPlate(_, plate))

        StreamParser(json.Parser(parserPlate, mode))(Chunk.buffer)

      case sv: DataFormat.SeparatedValues =>
        val cfg = csv.Parser.Config(
          header = sv.header,
          row1 = sv.row1.toByte,
          row2 = sv.row2.toByte,
          record = sv.record.toByte,
          openQuote = sv.openQuote.toByte,
          closeQuote = sv.closeQuote.toByte,
          escape = sv.escape.toByte)

        val parserPlate: F[Plate[ArrayBuffer[A]]] =
          QDataPlate[F, A, ArrayBuffer[A]](false).map(
            MultiplexingPlate(_, plate))

        StreamParser(csv.Parser(parserPlate, cfg))(Chunk.buffer)
    }

  private def unzipP[F[_]](
      bec: Blocker,
      chunkSize: Int = DefaultDecompressionBufferSize)(
      implicit F: ConcurrentEffect[F], 
      cs: ContextShift[F])
      : Pipe[F, Byte, (String, Stream[F, Byte])] = {

    def entry(zis: ZipInputStream): OptionT[F, (String, Stream[F, Byte])] =
      OptionT(Sync[F].delay(Option(zis.getNextEntry()))).map { ze =>
        (ze.getName, io.readInputStream[F](F.delay(zis), DefaultDecompressionBufferSize, bec, closeAfterUse = false))
      }

    def unzipEntries(zis: ZipInputStream): Stream[F, (String, Stream[F, Byte])] =
      Stream.unfoldEval(zis) { zis0 =>
        entry(zis0).map((_, zis0)).value
      }

    value: Stream[F, Byte] =>
      value.through(io.toInputStream).flatMap { is: InputStream =>
        val zis: F[ZipInputStream]          = Sync[F].delay(new ZipInputStream(is))
        val zres: Stream[F, ZipInputStream] = Stream.bracket(zis)(zis => Sync[F].delay(zis.close()))
        zres.flatMap { z =>
          unzipEntries(z)
        }
      }
   }

  private def unzip[F[_]](zipped: Stream[F, Byte], bec: Blocker, chunkSize: Int = DefaultDecompressionBufferSize)(
      implicit F: ConcurrentEffect[F],
      cs: ContextShift[F])
      : Stream[F, (String, Stream[F, Byte])] =
    zipped.through(unzipP(bec, chunkSize))

}
