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

package quasar.impl.datasource

import slamdata.Predef.{Boolean, Option}

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.PhysicalDatasource

import scalaz.{Applicative, PlusEmpty}
import scalaz.std.option._
import scalaz.syntax.applicative._
import scalaz.syntax.plusEmpty._

final class EmptyDatasource[F[_]: Applicative, G[_]: PlusEmpty, Q, R] private (
    val kind: DatasourceType,
    emptyResult: R)
    extends PhysicalDatasource[F, G, Q, R] {

  def evaluate(q: Q): F[R] =
    emptyResult.pure[F]

  def pathIsResource(path: ResourcePath): F[Boolean] =
    false.pure[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType.Physical)]]] =
    ResourcePath.root
      .getOption(prefixPath)
      .as(mempty[G, (ResourceName, ResourcePathType.Physical)])
      .pure[F]
}

object EmptyDatasource {
  def apply[F[_]: Applicative, G[_]: PlusEmpty, Q, R](
      kind: DatasourceType,
      emptyResult: R)
      : PhysicalDatasource[F, G, Q, R] =
    new EmptyDatasource[F, G, Q, R](kind, emptyResult)
}
