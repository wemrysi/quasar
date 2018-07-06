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

import slamdata.Predef.{List, None, Option, Some, Unit}
import quasar.Condition
import quasar.api.ResourceName
import quasar.api.datasource.{DatasourceConfig, DatasourceType}
import quasar.api.datasource.DatasourceError._
import quasar.contrib.scalaz.{MonadState_, MonadTell_}
import MockDatasourceControl.{Initialized, MonadInit, MonadShutdown, Shutdowns}

import scalaz.{ISet, Monad}
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._

/** Provides for control over the lifecycle of external Datasources. */
final class MockDatasourceControl[F[_]: Monad: MonadInit: MonadShutdown, C] private (
    supportedTypes: ISet[DatasourceType],
    initErrors: C => Option[InitializationError[C]])
    extends DatasourceControl[F, C] {

  private val initd = MonadState_[F, Initialized]
  private val sdown = MonadTell_[F, Shutdowns]

  def init(
      name: ResourceName,
      config: DatasourceConfig[C])
      : F[Condition[CreateError[C]]] =
    if (supportedTypes.member(config.kind))
      initErrors(config.config) match {
        case Some(err) =>
          Condition.abnormal[CreateError[C]](err).point[F]

        case None =>
          for {
            inits <- initd.get
            _ <- inits.member(name).whenM(shutdown(name))
            _ <- initd.put(inits.insert(name))
          } yield Condition.normal[CreateError[C]]()
      }
    else
      Condition.abnormal[CreateError[C]](
        DatasourceUnsupported(config.kind, supportedTypes))
        .point[F]

  def shutdown(name: ResourceName): F[Unit] =
    sdown.tell(List(name)) >> initd.modify(_.delete(name))

  def rename(src: ResourceName, dst: ResourceName): F[Unit] =
    for {
      inits <- initd.get
      _ <- inits.member(dst).whenM(shutdown(dst))
      _ <- initd.put(inits.delete(src).insert(dst))
    } yield ()

  def supported: F[ISet[DatasourceType]] =
    supportedTypes.point[F]
}

object MockDatasourceControl {
  type Initialized = ISet[ResourceName]
  type MonadInit[F[_]] = MonadState_[F, Initialized]

  type Shutdowns = List[ResourceName]
  type MonadShutdown[F[_]] = MonadTell_[F, Shutdowns]

  def apply[F[_]: Monad: MonadInit: MonadShutdown, C](
      supportedTypes: ISet[DatasourceType],
      initErrors: C => Option[InitializationError[C]])
      : DatasourceControl[F, C] =
    new MockDatasourceControl[F, C](supportedTypes, initErrors)
}
