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

package quasar.mimir

import quasar.Disposable
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError.InitializationError
import quasar.api.resource.ResourcePath
import quasar.connector._
import quasar.impl.datasource.local._

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, Timer}
import cats.syntax.functor._
import eu.timepit.refined.auto._
import fs2.{gzip, Stream}
import scalaz.\/
import shims._

/** A local datasource that gzips all resources. */
object GzippedLocalDatasourceModule extends LightweightDatasourceModule {
  val kind = DatasourceType("local-gzipped", 1L)

  def sanitizeConfig(config: Json) =
    LocalDatasourceModule.sanitizeConfig(config)

  def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
      config: Json)(
      implicit ec: ExecutionContext)
      : F[InitializationError[Json] \/ Disposable[F, Datasource[F, Stream[F, ?], ResourcePath, QueryResult[F]]]] = {

    val qeLens = Datasource.evaluator[F, Stream[F, ?], ResourcePath, QueryResult[F]]

    LocalDatasourceModule.lightweightDatasource[F](config).map(_.map(_.map(qeLens modify { qe =>
      qe map {
        case QueryResult.Typed(tpe, bytes) =>
          QueryResult.compressed(
            CompressionScheme.Gzip,
            QueryResult.typed(
              tpe,
              bytes.through(gzip.compress[F](32768))))

        case other => other
      }
    })))
  }
}
