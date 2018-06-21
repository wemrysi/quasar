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

package quasar.api

import slamdata.Predef.{None, Some}
import quasar.api.DataSourceError.{CommonError, CreateError, DataSourceNotFound, ExistentialError, InitializationError}
import quasar.Condition
import quasar.contrib.scalaz.MonadState_
import scalaz.{IMap, ISet, Monad, \/}
import scalaz.syntax.monad._
import scalaz.syntax.equal._
import scalaz.syntax.std.option._
import MockDataSources.DSMockState


final class MockDataSources[F[_]: Monad: DSMockState[?[_], C], C] private (
  supportedDataSources: ISet[DataSourceType],
  errorCondition: (ResourceName, DataSourceType, C) => Condition[InitializationError[C]])
  extends DataSources[F, C] {

  val store = MonadState_[F, IMap[ResourceName, (DataSourceMetadata, C)]]

  def add(
      name: ResourceName,
      kind: DataSourceType,
      config: C,
      onConflict: ConflictResolution)
      : F[Condition[CreateError[C]]]=
    if (supportedDataSources.contains(kind))
      store.get.flatMap(m => m.lookup(name) match {
        case Some(_) if onConflict === ConflictResolution.Preserve =>
          Condition.abnormal[CreateError[C]](DataSourceError.DataSourceExists(name)).point[F]
        case _ => errorCondition(name, kind, config) match {
            case Condition.Abnormal(e) => Condition.abnormal[CreateError[C]](e).point[F]
            case Condition.Normal() => store.put(m.insert(name, (DataSourceMetadata(kind, Condition.normal()), config))).as(Condition.normal())
          }
     })
    else
      Condition.abnormal[CreateError[C]](DataSourceError.DataSourceUnsupported(kind, supportedDataSources)).point[F]

  def lookup(name: ResourceName): F[CommonError \/ (DataSourceMetadata, C)] =
   store.gets(m => m.lookup(name).toRightDisjunction(DataSourceNotFound(name)))

  def metadata: F[IMap[ResourceName, DataSourceMetadata]] = store.gets(x => x.map(_._1))

  def remove(name: ResourceName): F[Condition[CommonError]] =
   store.gets(x => x.updateLookupWithKey(name, (_, _) => None)).flatMap {
     case (Some(_), m) =>
       store.put(m).as(Condition.normal())
     case (None, _) =>
       Condition.abnormal[CommonError](DataSourceNotFound(name)).point[F]
   }

  def rename(src: ResourceName, dst: ResourceName, onConflict: ConflictResolution): F[Condition[ExistentialError]] =
   store.get.flatMap(m => (m.lookup(src), m.lookup(dst)) match {
     case (None, _) => Condition.abnormal[ExistentialError](DataSourceNotFound(src)).point[F]
     case (Some(_), _) if src === dst =>
       Condition.normal().point[F]
     case (Some(_), Some(_)) if onConflict === ConflictResolution.Preserve =>
       Condition.abnormal[ExistentialError](DataSourceError.DataSourceExists(dst)).point[F]
     case (Some(s), _) => store.put(m.delete(src).insert(dst, s)).as(Condition.normal())
   })

  def supported: F[ISet[DataSourceType]] = supportedDataSources.point[F]
}

object MockDataSources {

  type DSMockState[F[_], C] = MonadState_[F, IMap[ResourceName, (DataSourceMetadata, C)]]

  def apply[F[_]: Monad: DSMockState[?[_], C], C](
      supportedDataSources:  ISet[DataSourceType],
      errorCondition: (ResourceName, DataSourceType, C) => Condition[InitializationError[C]]
      ): DataSources[F, C] =
    new MockDataSources[F, C](supportedDataSources, errorCondition)
}
