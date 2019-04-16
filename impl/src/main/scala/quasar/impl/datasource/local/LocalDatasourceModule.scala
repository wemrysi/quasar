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
import quasar.concurrent.BlockingContext
import quasar.connector.{LightweightDatasourceModule, DestinationModule, MonadResourceErr}

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect._
import scalaz.\/
import scalaz.syntax.applicative._
import shims._

object LocalDatasourceModule extends LightweightDatasourceModule with DestinationModule {
  // FIXME this is side effecting
  private lazy val blockingPool: BlockingContext =
    BlockingContext.cached("local-datasource")

  val kind: DatasourceType = LocalType
  val destinationKind: DestinationType = LocalDestinationType

  def sanitizeConfig(config: Json): Json = config
  def sanitizeDestinationConfig(config: Json): Json = config

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)(
      implicit ec: ExecutionContext)
      : F[InitializationError[Json] \/ Disposable[F, DS[F]]] = {
    val ds = for {
      lc <- attemptConfig[F, LocalConfig](config, "Failed to decode LocalDatasource config: ")
      root <- validatePath(lc.rootDir, config, "Invalid path: ")
    } yield LocalDatasource[F](root, lc.readChunkSizeBytes, blockingPool).point[Disposable[F, ?]]

    ds.run
  }

  def destination[F[_]: Effect: ContextShift: MonadResourceErr](config: Json)
      : F[InitializationError[Json] \/ Disposable[F, Dest[F]]] =
    mkDestination(config)
}
