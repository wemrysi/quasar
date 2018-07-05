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

import slamdata.Predef.{Boolean, Unit}
import quasar.api._, ResourceError._
import quasar.connector.DataSource
import quasar.contrib.scalaz.MonadError_

import scalaz.{\/, Applicative}

final class FailedDataSource[
    E,
    F[_]: Applicative: MonadError_[?[_], E],
    G[_], Q, R] private (
    dataSourceType: DataSourceType,
    error: E)
    extends DataSource[F, G, Q, R] {

  val kind: DataSourceType = dataSourceType

  val shutdown: F[Unit] = Applicative[F].point(())

  def evaluate(query: Q): F[ReadError \/ R] =
    MonadError_[F, E].raiseError(error)

  def children(path: ResourcePath): F[CommonError \/ G[(ResourceName, ResourcePathType)]] =
    MonadError_[F, E].raiseError(error)

  def descendants(path: ResourcePath): F[CommonError \/ G[ResourcePath]] =
    MonadError_[F, E].raiseError(error)

  def isResource(path: ResourcePath): F[Boolean] =
    MonadError_[F, E].raiseError(error)
}

object FailedDataSource {
  def apply[
      E,
      F[_]: Applicative: MonadError_[?[_], E],
      G[_], Q, R](
      kind: DataSourceType,
      error: E)
      : DataSource[F, G, Q, R] =
    new FailedDataSource[E, F, G, Q, R](kind, error)
}
