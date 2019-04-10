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

import quasar.Disposable
import quasar.api.datasource.{DatasourceError, DatasourceType}, DatasourceError._
import quasar.common.data.RValue
import quasar.concurrent.BlockingContext
import quasar.connector.{Capability, ReadWrite, LightweightDatasourceModule, MonadResourceErr}

import java.nio.file.Paths
import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect._
import scalaz.{EitherT, \/}
import scalaz.syntax.applicative._
import shims._

object LocalParsedDatasourceModule extends LightweightDatasourceModule {
  // FIXME this is side effecting
  private lazy val blockingPool: BlockingContext =
    BlockingContext.cached("local-parsed-datasource")

  val kind: DatasourceType = LocalParsedType
  val capability: Capability = ReadWrite

  def sanitizeConfig(config: Json): Json = config

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)(
      implicit ec: ExecutionContext)
      : F[InitializationError[Json] \/ Disposable[F, DS[F]]] = {

    val F = ConcurrentEffect[F]

    val ds = for {
      lc <-
        EitherT.fromEither(config.as[LocalConfig].toEither.point[F])
          .leftMap[InitializationError[Json]] {
            case (s, _) => MalformedConfiguration(kind, config, "Failed to decode LocalDatasource config: " + s)
          }

      root <-
        EitherT.fromEither(F.attempt(F.delay(Paths.get(lc.rootDir))))
          .leftMap[InitializationError[Json]](
            t => MalformedConfiguration(kind, config, "Invalid path: " + t.getMessage))
    } yield LocalParsedDatasource[F, RValue](root, lc.readChunkSizeBytes, blockingPool).point[Disposable[F, ?]]

    ds.run
  }

  def destination[F[_]: Effect: ContextShift: MonadResourceErr](
      config: Json)
      : F[InitializationError[Json] \/ Disposable[F, Dest[F]]] = {
    val F = Effect[F]
    val dest = for {
      lc <-
        EitherT.fromEither(config.as[LocalDestinationConfig].toEither.point[F])
          .leftMap[InitializationError[Json]] {
            case (s, _) => MalformedConfiguration(kind, config, "Failed to decode LocalDestination config: " + s)
          }

      root <-
        EitherT.fromEither(F.attempt(F.delay(Paths.get(lc.rootDir))))
          .leftMap[InitializationError[Json]](
            t => MalformedConfiguration(kind, config, "Invalid destination path: " + t.getMessage))

      localDest: Dest[F] = LocalDestination[F](root, blockingPool)
    } yield localDest.point[Disposable[F, ?]]

    dest.run
  }
}
