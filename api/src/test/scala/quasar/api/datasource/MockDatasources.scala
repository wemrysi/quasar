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

package quasar.api.datasource

import slamdata.Predef.{None, Some}
import quasar.Condition
import quasar.api.ResourceName
import quasar.contrib.scalaz.MonadState_

import scalaz.{IMap, ISet, Monad, \/}
import scalaz.syntax.monad._
import scalaz.syntax.equal._
import scalaz.syntax.std.option._

import DatasourceError.{CommonError, CreateError, DatasourceNotFound, ExistentialError, InitializationError}
import MockDatasources.DSMockState

final class MockDatasources[F[_]: Monad: DSMockState[?[_], C], C] private (
  supportedDatasources: ISet[DatasourceType],
  errorCondition: (ResourceName, DatasourceType, C) => Condition[InitializationError[C]])
  extends Datasources[F, C] {

  val store = MonadState_[F, IMap[ResourceName, (DatasourceMetadata, C)]]

  def add(
      name: ResourceName,
      kind: DatasourceType,
      config: C,
      onConflict: ConflictResolution)
      : F[Condition[CreateError[C]]]=
    if (supportedDatasources.contains(kind))
      store.get.flatMap(m => m.lookup(name) match {
        case Some(_) if onConflict === ConflictResolution.Preserve =>
          Condition.abnormal[CreateError[C]](DatasourceError.DatasourceExists(name)).point[F]
        case _ => errorCondition(name, kind, config) match {
            case Condition.Abnormal(e) => Condition.abnormal[CreateError[C]](e).point[F]
            case Condition.Normal() => store.put(m.insert(name, (DatasourceMetadata(kind, Condition.normal()), config))).as(Condition.normal())
          }
     })
    else
      Condition.abnormal[CreateError[C]](DatasourceError.DatasourceUnsupported(kind, supportedDatasources)).point[F]

  def lookup(name: ResourceName): F[CommonError \/ (DatasourceMetadata, C)] =
   store.gets(m => m.lookup(name).toRightDisjunction(DatasourceNotFound(name)))

  def metadata: F[IMap[ResourceName, DatasourceMetadata]] = store.gets(x => x.map(_._1))

  def remove(name: ResourceName): F[Condition[CommonError]] =
   store.gets(x => x.updateLookupWithKey(name, (_, _) => None)).flatMap {
     case (Some(_), m) =>
       store.put(m).as(Condition.normal())
     case (None, _) =>
       Condition.abnormal[CommonError](DatasourceNotFound(name)).point[F]
   }

  def rename(src: ResourceName, dst: ResourceName, onConflict: ConflictResolution): F[Condition[ExistentialError]] =
   store.get.flatMap(m => (m.lookup(src), m.lookup(dst)) match {
     case (None, _) => Condition.abnormal[ExistentialError](DatasourceNotFound(src)).point[F]
     case (Some(_), _) if src === dst =>
       Condition.normal().point[F]
     case (Some(_), Some(_)) if onConflict === ConflictResolution.Preserve =>
       Condition.abnormal[ExistentialError](DatasourceError.DatasourceExists(dst)).point[F]
     case (Some(s), _) => store.put(m.delete(src).insert(dst, s)).as(Condition.normal())
   })

  def supported: F[ISet[DatasourceType]] = supportedDatasources.point[F]
}

object MockDatasources {

  type DSMockState[F[_], C] = MonadState_[F, IMap[ResourceName, (DatasourceMetadata, C)]]

  def apply[F[_]: Monad: DSMockState[?[_], C], C](
      supportedDatasources:  ISet[DatasourceType],
      errorCondition: (ResourceName, DatasourceType, C) => Condition[InitializationError[C]]
      ): Datasources[F, C] =
    new MockDatasources[F, C](supportedDatasources, errorCondition)
}
