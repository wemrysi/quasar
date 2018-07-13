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

package quasar.connector

import quasar.Data
import quasar.api.{DataSourceType, ResourcePath}
import quasar.api.DataSourceError.InitializationError

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import fs2.Stream
import scalaz.\/

trait LightweightDataSourceModule {
  def kind: DataSourceType

  def lightweightDataSource[
      F[_]: ConcurrentEffect: Timer,
      G[_]: ConcurrentEffect: Timer](
      config: Json)
      : F[InitializationError[Json] \/ DataSource[F, Stream[G, ?], ResourcePath, Stream[G, Data]]]
}
