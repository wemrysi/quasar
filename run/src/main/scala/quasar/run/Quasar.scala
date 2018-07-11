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

package quasar.run

import quasar.Data
import quasar.api.{DataSources, QueryEvaluator, ResourceDiscovery, ResourceName}
import quasar.common.PhaseResultTell
import quasar.contrib.fs2.stream._
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.MonadError_
import quasar.evaluate.FederatingQueryEvaluator
import quasar.impl.DataSourceModule
import quasar.impl.datasource.local.LocalDataSourceModule
import quasar.impl.datasources.{DataSourceConfig, DataSourceManagement, DefaultDataSources}
import quasar.impl.external.{ExternalConfig, ExternalDataSources}
import quasar.mimir.Precog
import quasar.mimir.datasources.MimirDataSourceConfigs
import quasar.mimir.evaluate.{MimirQueryFederation, QueryAssociate}
import quasar.run.data.{jsonToRValue, rValueToJson}
import quasar.run.implicits._
import quasar.yggdrasil.vfs.ResourceError

import java.nio.file.Path
import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.~>
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.functor._
import cats.syntax.flatMap._
import fs2.{Scheduler, Stream}
import matryoshka.data.Fix
import pathy.Path._
import scalaz.IMap
import scalaz.syntax.foldable._
import scalaz.syntax.invariantFunctor._
import shims._

final class Quasar[F[_], G[_]](
    val dataSources: DataSources[F, Json],
    val queryEvaluator: QueryEvaluator[F, Stream[G, ?], SqlQuery, Stream[G, Data]])

object Quasar {
  // The location the datasource configs table within `mimir`.
  val DataSourceConfigsLocation: AFile =
    rootDir </> dir("quasar") </> file("datasource-configs")

  /** What it says on the tin.
    *
    * @param mimirDir directory where mimir should store its data
    * @param extConfig datasource plugin configuration
    */
  def apply[F[_]: ConcurrentEffect: MonadQuasarErr: PhaseResultTell: Timer](
      mimirDir: Path,
      extConfig: ExternalConfig,
      pool: ExecutionContext)
      : Stream[F, Quasar[F, IO]] = {

    implicit val ec = pool

    for {
      precog <- Stream.bracket(Precog(mimirDir.toFile).to[F])(
        d => Stream.emit(d.unsafeValue),
        _.dispose.to[F])

      configs =
        MimirDataSourceConfigs[F](precog, DataSourceConfigsLocation)
          .xmap(rValueToJson, jsonToRValue)

      configStream <- Stream.eval(MonadError_[F, ResourceError] unattempt {
        MimirDataSourceConfigs.allConfigs(precog, DataSourceConfigsLocation).run.to[F]
      })

      configured <-
        configStream
          .map { case (n, c) => (n, c.map(rValueToJson)) }
          .fold(IMap.empty[ResourceName, DataSourceConfig[Json]])(_ + _)
          .translate(λ[IO ~> F](_.to[F]))

      extMods <- ExternalDataSources[F](extConfig, pool)

      modules = extMods.insert(
        LocalDataSourceModule.kind,
        DataSourceModule.Lightweight(LocalDataSourceModule))

      scheduler <- Scheduler(corePoolSize = 1, threadPrefix = "quasar-scheduler")

      mr <- Stream.bracket(DataSourceManagement[Fix, F, IO](modules, configured, pool, scheduler))(
        Stream.emit(_),
        { case (_, r) => r.get.flatMap(_.traverse_(_.fold(_.shutdown, _.shutdown))) })

      (mgmt, running) = mr

      dataSources = DefaultDataSources(configs, mgmt, mgmt)

      federation = MimirQueryFederation[Fix, F](precog)

      sources = running.get.map(_.map(ds => (
        ds.fold[ResourceDiscovery[F, Stream[IO, ?]]](
          l => l,
          h => h),
        ds.fold[QueryAssociate[Fix, F, IO]](
          lw => QueryAssociate.Lightweight(lw.evaluate),
          hw => QueryAssociate.Heavyweight(hw.evaluate)))))

      queryEvaluator =
        Sql2QueryEvaluator(FederatingQueryEvaluator(federation, sources))

    } yield new Quasar(dataSources, queryEvaluator)
  }
}
