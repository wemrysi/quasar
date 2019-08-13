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

package quasar.impl.datasource.local

import slamdata.Predef._

import quasar.concurrent.BlockingContext
import quasar.connector._, LightweightDatasourceModule.DS
import quasar.impl.parsing.ResultParser

import java.nio.file.{Path => JPath}

import cats.effect.{ContextShift, Effect, Timer}
import fs2.{gzip, io}
import qdata.{QDataDecode, QDataEncode}
import scalaz.syntax.tag._

object LocalParsedDatasource {

  val DecompressionBufferSize: Int = 32768

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer, A: QDataDecode: QDataEncode](
      root: JPath,
      readChunkSizeBytes: Int,
      format: ParsableType,
      compressionScheme: Option[CompressionScheme],
      blockingPool: BlockingContext)
      : DS[F] = {

    EvaluableLocalDatasource[F](LocalParsedType, root) { iRead =>
      val rawBytes =
        io.file.readAll[F](iRead.path, blockingPool.unwrap, readChunkSizeBytes)

      val decompressedBytes = compressionScheme match {
        case Some(CompressionScheme.Gzip) =>
          rawBytes.through(gzip.decompress[F](DecompressionBufferSize))

        case None =>
          rawBytes
      }

      val parsedValues = decompressedBytes.through(ResultParser.parsableTypePipe(format))

      QueryResult.parsed[F, A](QDataDecode[A], parsedValues, iRead.stages)
    }
  }
}
