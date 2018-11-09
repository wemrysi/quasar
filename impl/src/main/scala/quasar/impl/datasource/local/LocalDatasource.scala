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

import quasar.api.resource.ResourcePath
import quasar.concurrent.BlockingContext
import quasar.connector.{Datasource, MonadResourceErr, ParsableType, QueryResult}

import java.nio.file.{Path => JPath}

import cats.effect.{ContextShift, Effect, Timer}
import fs2.{io, Stream}
import scalaz.syntax.tag._

object LocalDatasource {

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer](
      root: JPath,
      readChunkSizeBytes: Int,
      blockingPool: BlockingContext)
      : Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F, Byte]] = {

    import ParsableType.JsonVariant

    EvaluableLocalDatasource[F, Byte](LocalType, root) { path =>
      QueryResult.typed(
        ParsableType.json(JsonVariant.LineDelimited, true),
        io.file.readAll[F](path, blockingPool.unwrap, readChunkSizeBytes))
    }
  }
}
