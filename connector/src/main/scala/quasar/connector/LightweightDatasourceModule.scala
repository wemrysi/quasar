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

import slamdata.Predef.Byte

import quasar.Disposable
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.resource.ResourcePath
import quasar.qscript.InterpretedRead

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import fs2.Stream
import scalaz.\/

trait LightweightDatasourceModule {
  type DS[F[_]] = Datasource[F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F]]
  type Dest[F[_]] = Destination[F, Stream[F, ?], Stream[F, Byte]]

  def kind: DatasourceType

  def sanitizeConfig(config: Json): Json

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)(
      implicit ec: ExecutionContext)
      : F[InitializationError[Json] \/ Disposable[F, DS[F]]]

  // val capability: Capability

  // def destination[F[_]: Effect: ContextShift: MonadResourceErr](
  //     config: Json)
  //     : F[InitializationError[Json] \/ Disposable[F, Dest[F]]]
}
