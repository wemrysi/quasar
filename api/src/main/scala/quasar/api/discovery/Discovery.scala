/*
 * Copyright 2020 Precog Data
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

package quasar.api.discovery

import quasar.api.resource._

import scala.Boolean
import scala.util.Either

trait Discovery[F[_], G[_], I, S <: SchemaConfig] {
  import DiscoveryError.DatasourceNotFound

  type PathType <: ResourcePathType

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(datasourceId: I, path: ResourcePath)
      : F[Either[DatasourceNotFound[I], Boolean]]

  /** Returns the name and type of the `ResourcePath`s within the specified
    * datasource implied by concatenating each name to `prefixPath`.
    */
  def prefixedChildPaths(datasourceId: I, prefixPath: ResourcePath)
      : F[Either[DiscoveryError[I], G[(ResourceName, PathType)]]]

  /** Retrieves the schema of the resource at the given `path`. */
  def resourceSchema(datasourceId: I, path: ResourcePath, schemaConfig: S)
      : F[Either[DiscoveryError[I], schemaConfig.Schema]]
}
