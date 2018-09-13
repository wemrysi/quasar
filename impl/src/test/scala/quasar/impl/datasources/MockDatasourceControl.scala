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

package quasar.impl.datasources

import slamdata.Predef.{Boolean, List, None, Option, Some, Unit}
import quasar.Condition
import quasar.api.MockSchemaConfig
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.contrib.scalaz.{MonadState_, MonadTell_}
import MockDatasourceControl.{MonadInit, MonadShutdown}

import scala.concurrent.duration.FiniteDuration

import scalaz.{\/, ISet, Monad, Order, PlusEmpty}
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.plusEmpty._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

/** Provides for control over the lifecycle of external Datasources. */
final class MockDatasourceControl[F[_]: Monad, G[_]: PlusEmpty, I: Order, C] private (
    supportedTypes: ISet[DatasourceType],
    initErrors: C => Option[InitializationError[C]],
    sanitize: C => C)(
    implicit initd: MonadInit[F, I], sdown: MonadShutdown[F, I])
    extends DatasourceControl[F, G, I, C, MockSchemaConfig.type] {

  def sanitizeRef(ref: DatasourceRef[C]): DatasourceRef[C] =
    ref.copy(config = sanitize(ref.config))

  def initDatasource(
      datasourceId: I,
      ref: DatasourceRef[C])
      : F[Condition[CreateError[C]]] =
    if (supportedTypes.member(ref.kind))
      initErrors(ref.config) match {
        case Some(err) =>
          Condition.abnormal[CreateError[C]](err).point[F]

        case None =>
          for {
            inits <- initd.get
            _ <- inits.member(datasourceId).whenM(shutdownDatasource(datasourceId))
            _ <- initd.put(inits.insert(datasourceId))
          } yield Condition.normal[CreateError[C]]()
      }
    else
      Condition.abnormal[CreateError[C]](
        DatasourceUnsupported(ref.kind, supportedTypes))
        .point[F]

  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean] =
    initd.gets(_.member(datasourceId)) map { exists =>
      if (exists) false.right
      else datasourceNotFound[I, ExistentialError[I]](datasourceId).left
    }

  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ G[(ResourceName, ResourcePathType)]] =
    initd.gets(_.member(datasourceId)) map { exists =>
      if (exists) mempty[G, (ResourceName, ResourcePathType)].right
      else datasourceNotFound[I, DiscoveryError[I]](datasourceId).left
    }

  def resourceSchema(
      datasourceId: I,
      path: ResourcePath,
      cfg: MockSchemaConfig.type,
      timeLimit: FiniteDuration)
      : F[DiscoveryError[I] \/ Option[cfg.Schema]] =
    initd.gets(_.member(datasourceId)) map { exists =>
      if (exists) MockSchemaConfig.MockSchema.some.right
      else datasourceNotFound[I, DiscoveryError[I]](datasourceId).left
    }

  def shutdownDatasource(datasourceId: I): F[Unit] =
    sdown.tell(List(datasourceId)) >> initd.modify(_.delete(datasourceId))

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    supportedTypes.point[F]
}

object MockDatasourceControl {
  type Initialized[I] = ISet[I]
  type MonadInit[F[_], I] = MonadState_[F, Initialized[I]]

  type Shutdowns[I] = List[I]
  type MonadShutdown[F[_], I] = MonadTell_[F, Shutdowns[I]]

  def apply[F[_]: Monad, G[_]: PlusEmpty, I: Order, C](
      supportedTypes: ISet[DatasourceType],
      initErrors: C => Option[InitializationError[C]],
      sanitize: C => C)(
      implicit MI: MonadInit[F, I], MS: MonadShutdown[F, I])
      : DatasourceControl[F, G, I, C, MockSchemaConfig.type] =
    new MockDatasourceControl[F, G, I, C](supportedTypes, initErrors, sanitize)
}
