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

package quasar.impl.datasource.local

import slamdata.Predef._

import cats.effect.{Sync, ContextShift, Effect, Timer}
import cats.syntax.functor._
import cats.syntax.foldable._
import cats.syntax.flatMap._

import eu.timepit.refined.auto._

import fs2.{io, Stream, Chunk}

import java.nio.file.{Path => JPath}

import jawn.Facade
import jawnfs2._

import quasar.ParseInstruction
import quasar.api.resource.ResourcePath
import quasar.common.data.RValue
import quasar.concurrent.BlockingContext
import quasar.connector.{Datasource, MonadResourceErr, QueryResult}
import quasar.impl.evaluate.RValueParseInstructionInterpreter
import quasar.qscript.InterpretedRead

import qdata.{QData, QDataDecode, QDataEncode}
import qdata.json.QDataFacade

import scala.collection.mutable.ArrayBuffer

import scalaz.syntax.tag._
import shims._

object LocalInterpretedReadDatasource  {
  final case class Interpreter[F[_], A](
      chunkParser: (List[ParseInstruction], Chunk[A]) => F[Chunk[A]],
      remainingInstructions: List[ParseInstruction] => List[ParseInstruction])

  private def isMask(a: ParseInstruction): Boolean = a match {
    case ParseInstruction.Mask(_) => true
    case _ => false
  }

  def filteredInterpreter[F[_]: Sync, A: QDataEncode: QDataDecode](f: ParseInstruction => Boolean) = {
    def chunkParser(is: List[ParseInstruction], chunk: Chunk[A]): F[Chunk[A]] = {
      val filtered = is.takeWhile(f)
      for {
        interpreter <- RValueParseInstructionInterpreter[F](filtered)
        res <- chunk.foldM(new ArrayBuffer[RValue](chunk.size))((buf, a) =>
          interpreter(QData.convert[A, RValue](a)).map(buf ++= _)
        ).map(x => Chunk.buffer(x.map(QData.convert[RValue, A](_))))
      } yield res
    }
    Interpreter(chunkParser, _.dropWhile(f))
  }

  def fullInterpreter[F[_]: Sync, A: QDataEncode: QDataDecode] =
    filteredInterpreter(x => true)

  def onlyMaskInterpreter[F[_]: Sync, A: QDataEncode: QDataDecode] =
    filteredInterpreter(isMask)

  def noMaskInterpreter[F[_]: Sync, A: QDataEncode: QDataDecode] =
    filteredInterpreter(!isMask(_))

  def noInterpreter[F[_]: Sync, A: QDataEncode: QDataDecode] =
    filteredInterpreter(x => false)

  def interpretStream[F[_]: Sync, A: QDataEncode: QDataDecode] (
      stream: Stream[F, A], interpreter: Interpreter[F, A], instructions: List[ParseInstruction])
      : Stream[F, A] = {
    stream.chunks.flatMap{ chunk => Stream.evalUnChunk(interpreter.chunkParser(instructions, chunk)) }
  }

  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer, A: QDataDecode: QDataEncode](
      root: JPath,
      readChunkSizeBytes: Int,
      blockingPool: BlockingContext,
      interpret: Interpreter[F, A])
      : Datasource[F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F]] = {

    implicit val facade: Facade[A] = QDataFacade(isPrecise = true)

    EvaluableLocalDatasource[F](LocalParsedType, root) { iRead =>
      val jsonStream =
        io.file.readAll[F](iRead.path, blockingPool.unwrap, readChunkSizeBytes)
          .chunks
          .map(_.toByteBuffer)
          .parseJsonStream[A]

      QueryResult.parsed[F, A](
        QDataDecode[A],
        interpretStream[F, A](jsonStream, interpret, iRead.instructions),
        interpret.remainingInstructions(iRead.instructions))
    }
  }
}
