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

import slamdata.Predef.{Boolean, Option, Unit}
import quasar.Condition
import quasar.api.SchemaConfig
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError.{CreateError, DiscoveryError, ExistentialError}
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}

import scala.concurrent.duration.FiniteDuration

import scalaz.{\/, ISet}

/** A primitive facility for managing the lifecycle of datasources and
  * discovering resources.
  *
  * All mutating operations replace existing datasources as this is intended
  * to be used as a dependency in another context, such as `DefaultDatasources`,
  * that decides whether doing so is appropriate.
  */
trait DatasourceControl[F[_], G[_], I, C, S <: SchemaConfig] {
  /** Initialize a datasource as `datasourceId` using the provided `ref`. If a
    * datasource exists at `datasourceId`, it is shut down.
    */
  def initDatasource(datasourceId: I, ref: DatasourceRef[C])
      : F[Condition[CreateError[C]]]

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[ExistentialError[I] \/ Boolean]

  /** Returns the name and type of the `ResourcePath`s within the specified
    * Datasource implied by concatenating each name to `prefixPath`.
    */
  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[DiscoveryError[I] \/ G[(ResourceName, ResourcePathType)]]

  /** Retrieves the schema of the resource at the given `path` with the
    * specified datasource according to the provided `schemaConfig`.
    *
    * Execution time is limited to `timeLimit` and any available value will
    * be returned upon expiration. If a "complete" schema, as defined by the
    * specified config, is computed before the time limit, it will be returned
    * immediately.
    *
    * Returns `None` if the resource exists but a schema is not available.
    */
  def resourceSchema(
      datasourceId: I,
      path: ResourcePath,
      schemaConfig: S,
      timeLimit: FiniteDuration)
      : F[DiscoveryError[I] \/ Option[schemaConfig.Schema]]

  /** Stop the named datasource, discarding it and freeing any resources it may
    * be using.
    */
  def shutdownDatasource(datasourceId: I): F[Unit]

  /** The types of datasources supported. */
  def supportedDatasourceTypes: F[ISet[DatasourceType]]

  def sanitizeRef(ref: DatasourceRef[C]): DatasourceRef[C]
}
