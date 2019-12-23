/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError._
import quasar.api.resource.{ResourcePathType, ResourcePath}
import quasar.connector.Datasource
import quasar.contrib.scalaz.{MonadState_, MonadTell_, MonadError_}
import quasar.impl.datasource.EmptyDatasource
import quasar.qscript.InterpretedRead

import MockDatasourceManager._

import scalaz.{ISet, Monad, Order, PlusEmpty, Equal, IMap}
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.std.option._


final class MockDatasourceManager[I: Order, C: Equal, T[_[_]], F[_], G[_]: PlusEmpty, R] private (
    supportedTypes: ISet[DatasourceType],
    initErrors: C => Option[InitializationError[C]],
    sanitize: C => C,
    emptyResult: R)(
    implicit
    m: Monad[F],
    createError: MonadError_[F, CreateError[C]],
    dsm: MonadDSM[F, I, C],
    sdown: MonadShutdown[F, I])
    extends DatasourceManager[I, C, T, F, G, R, ResourcePathType] {

  private type DS = Datasource[F, G, InterpretedRead[ResourcePath], R, ResourcePathType]
  private type MDS = ManagedDatasource[T, F, G, R, ResourcePathType]

  def managedDatasource(id: I, fallback: Option[DatasourceRef[C]] = None): F[Option[MDS]] = {
    val act = for {
      MockDSMState(caches, refs) <- dsm.get
      cache = caches.lookup(id)
      mbref = refs.lookup(id) orElse fallback
      result <- mbref match {
        case Some(ref) =>
          val emptyDS: DS = EmptyDatasource(ref.kind, emptyResult)
          val fDS: F[DS] = if (supportedTypes.member(ref.kind)) {
            initErrors(ref.config) match {
              case Some(e) =>
                createError.raiseError(e)
              case None => cache match {
                case Some(current) if DatasourceRef.atMostRenamed(current, ref) =>
                  emptyDS.point[F]
                case Some(_) =>
                  for {
                  _ <- cache.nonEmpty.whenM(shutdownDatasource(id))
                  _ <- dsm.modify(x => x.copy(cache = x.cache.insert(id, ref)))
                } yield emptyDS
                case None =>
                  dsm.modify(x => x.copy(cache = x.cache.insert(id, ref))) as emptyDS
              }
            }
          } else {
            createError.raiseError(DatasourceUnsupported(ref.kind, supportedTypes))
          }
          fDS.map(x => ManagedDatasource.lightweight[T][F, G, R, ResourcePathType](x).some)
        case None =>
          cache.nonEmpty.whenM(shutdownDatasource(id)) as none[MDS]
      }
    } yield result
    act
  }

  def sanitizedRef(ref: DatasourceRef[C]): DatasourceRef[C] =
    ref.copy(config = sanitize(ref.config))

  def shutdownDatasource(datasourceId: I): F[Unit] =
    sdown.tell(List(datasourceId)) >> dsm.modify(x => x.copy(cache = x.cache.delete(datasourceId)))

  def supportedDatasourceTypes: F[ISet[DatasourceType]] =
    supportedTypes.point[F]
}

object MockDatasourceManager {
  final case class MockDSMState[I, C](cache: IMap[I, DatasourceRef[C]], refs: IMap[I, DatasourceRef[C]])
  type MonadDSM[F[_], I, C] = MonadState_[F, MockDSMState[I, C]]

  type Shutdowns[I] = List[I]
  type MonadShutdown[F[_], I] = MonadTell_[F, Shutdowns[I]]

  def apply[I: Order, C: Equal, T[_[_]], F[_]: Monad, G[_]: PlusEmpty, R](
      supportedTypes: ISet[DatasourceType],
      initErrors: C => Option[InitializationError[C]],
      sanitize: C => C,
      emptyResult: R)(
      implicit
      ME: MonadError_[F, CreateError[C]],
      MC: MonadDSM[F, I, C],
      MS: MonadShutdown[F, I])
      : DatasourceManager[I, C, T, F, G, R, ResourcePathType] =
    new MockDatasourceManager[I, C, T, F, G, R](supportedTypes, initErrors, sanitize, emptyResult)
}
