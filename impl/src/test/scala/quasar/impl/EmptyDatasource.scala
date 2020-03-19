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

package quasar.impl

import slamdata.Predef.{Boolean, Option}

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Offset
import quasar.connector.datasource._

import cats.{Applicative, MonoidK}
import cats.data.NonEmptyList
import cats.implicits._

final class EmptyDatasource[F[_]: Applicative, G[_]: MonoidK, Q, R, P <: ResourcePathType] private (
    val kind: DatasourceType,
    emptyResult: R,
    supportsSeek: Boolean)
    extends Datasource[F, G, Q, R, P] {

  val loaders =
    NonEmptyList.of(Loader.Batch(
      if (supportsSeek)
        BatchLoader.Seek((q: Q, o: Option[Offset]) => emptyResult.pure[F])
      else
        BatchLoader.Full((q: Q) => emptyResult.pure[F])))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    false.pure[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    ResourcePath.root
      .getOption(prefixPath)
      .as(MonoidK[G].empty[(ResourceName, P)])
      .pure[F]
}

object EmptyDatasource {
  def apply[F[_]: Applicative, G[_]: MonoidK, Q, R, P <: ResourcePathType](
      kind: DatasourceType,
      emptyResult: R,
      supportsSeek: Boolean = false)
      : Datasource[F, G, Q, R, P] =
    new EmptyDatasource[F, G, Q, R, P](kind, emptyResult, supportsSeek)
}
