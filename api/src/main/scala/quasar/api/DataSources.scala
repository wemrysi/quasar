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

import quasar.Condition

import scalaz.{\/, IMap, ISet}

trait DataSources[F[_], C] {
  import DataSourceError._

  /** Returns the result of attempting to add a new datasource to the
    * set of datasources, `Unit` indicates the operation was successful.
    *
    * @name an identifier to assign to the datasource, must not exist unless `onConflict` is `Replace`
    * @kind uniquely identifies the type of datasource
    * @config configuration information, the specifics of which are determined by `kind`
    * @onConflict an enumeration describing how to resolve name conflicts
    */
  def add(
      name: ResourceName,
      kind: DataSourceType,
      config: C,
      onConflict: ConflictResolution)
      : F[Condition[CreateError[C]]]

  /** Returns the metadata and configuration for the specified datasource,
    * or an error if it doesn't exist.
    */
  def lookup(name: ResourceName): F[CommonError \/ (DataSourceMetadata, C)]

  /** Metadata for all datasources. */
  def metadata: F[IMap[ResourceName, DataSourceMetadata]]

  /** Removes the specified datasource, making its data unavailable.
    *
    * An error is returned if no datasource exists having the specified name.
    */
  def remove(name: ResourceName): F[Condition[CommonError]]

  /** Rename `src` to `dst`, handling conflicts at `dst` according to
    * `onConflict`. An error is returned if `src` does not exist.
    */
  def rename(
      src: ResourceName,
      dst: ResourceName,
      onConflict: ConflictResolution)
      : F[Condition[ExistentialError]]

  /** The set of supported datasources. */
  def supported: F[ISet[DataSourceType]]
}
