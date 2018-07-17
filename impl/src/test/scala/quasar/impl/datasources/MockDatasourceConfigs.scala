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

import slamdata.Predef.{Boolean, None, Option, Some, Unit}
import quasar.api.ResourceName
import quasar.api.datasource.DatasourceType
import quasar.contrib.scalaz.MonadState_
import quasar.fp.ski.κ2
import MockDatasourceConfigs.{Configs, MonadConfigs}

import scalaz.{IMap, Monad}
import scalaz.syntax.monad._

final class MockDatasourceConfigs[C, F[_]: Monad: MonadConfigs[?[_], C]] private ()
    extends DatasourceConfigs[F, C] {

  private val configs = MonadState_[F, Configs[C]]

  def add(name: ResourceName, config: DatasourceConfig[C]): F[Unit] =
    configs.modify(_.insert(name, config))

  def configured: F[IMap[ResourceName, DatasourceType]] =
    configs.gets(_.map(_.kind))

  def lookup(name: ResourceName): F[Option[DatasourceConfig[C]]] =
    configs.gets(_.lookup(name))

  def remove(name: ResourceName): F[Boolean] =
    for {
      cfgs <- configs.get

      upd = cfgs.updateLookupWithKey(name, κ2(None))

      (oldCfg, newCfgs) = upd

      _ <- configs.put(newCfgs)
    } yield oldCfg.isDefined

  def rename(src: ResourceName, dst: ResourceName): F[Unit] =
    configs.modify(
      _.updateLookupWithKey(src, κ2(None)) match {
        case (Some(cfg), m) => m.insert(dst, cfg)
        case (None, m) => m
      })
}

object MockDatasourceConfigs {
  type Configs[C] = IMap[ResourceName, DatasourceConfig[C]]
  type MonadConfigs[F[_], C] = MonadState_[F, Configs[C]]

  def apply[C, F[_]: Monad: MonadConfigs[?[_], C]]: DatasourceConfigs[F, C] =
    new MockDatasourceConfigs[C, F]()
}
