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

import slamdata.Predef.{Option, Unit, None}
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.resource.ResourcePathType

import scalaz.ISet

/** A primitive facility for managing the lifecycle of datasources. */
private[quasar] trait DatasourceManager[I, C, T[_[_]], F[_], G[_], R, P <: ResourcePathType] {
  /** Initialize the datasource if it hasn't been already, returns the managed datasource
      having the given id or `None` if not found
    */
  def managedDatasource(datasourceId: I, or: Option[DatasourceRef[C]] = None): F[Option[ManagedDatasource[T, F, G, R, P]]]

  /** Returns `ref` devoid of any sensitive information (credentials and the like). */
  def sanitizedRef(ref: DatasourceRef[C]): DatasourceRef[C]

  /** Stop the named datasource, discarding it and freeing any resources it may
    * be using.
    */
  def shutdownDatasource(datasourceId: I): F[Unit]

  /** The types of datasources supported. */
  def supportedDatasourceTypes: F[ISet[DatasourceType]]
}
