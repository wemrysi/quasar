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

package quasar.connector

import slamdata.Predef.{Boolean, Option}
import quasar.api.QueryEvaluator
import quasar.api.datasource.DatasourceType
import quasar.api.resource._

import qdata.QDataEncode

/** @tparam F effects
  * @tparam G multiple results
  * @tparam Q query
  */
trait Datasource[F[_], G[_], Q] {

  /** The query evaluator for this datasource. */
  def evaluator[R: QDataEncode]: QueryEvaluator[F, Q, G[R]]

  /** The type of this datasource. */
  def kind: DatasourceType

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(path: ResourcePath): F[Boolean]

  /** Returns the name and type of the `ResourcePath`s within the specified
    * Datasource implied by concatenating each name to `prefixPath`.
    */
  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]]
}
