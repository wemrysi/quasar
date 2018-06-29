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

import slamdata.Predef.{Boolean, None, Some, Unit}
import quasar.Condition
import quasar.api._, ResourceError._
import quasar.connector.DataSource
import quasar.contrib.scalaz.MonadError_

import scalaz.{\/, Monad}

final class ConditionReportingDataSource[
    E, F[_]: Monad: MonadError_[?[_], E], G[_], Q, R] private (
    report: Condition[E] => F[Unit],
    underlying: DataSource[F, G, Q, R])
    extends DataSource[F, G, Q, R] {

  val kind: DataSourceType = underlying.kind

  val shutdown: F[Unit] = underlying.shutdown

  def evaluate(query: Q): F[ReadError \/ R] =
    reportCondition(underlying.evaluate(query))

  def children(path: ResourcePath): F[CommonError \/ G[(ResourceName, ResourcePathType)]] =
    reportCondition(underlying.children(path))

  def descendants(path: ResourcePath): F[CommonError \/ G[ResourcePath]] =
    reportCondition(underlying.descendants(path))

  def isResource(path: ResourcePath): F[Boolean] =
    reportCondition(underlying.isResource(path))

  ////

  private def reportCondition[A](fa: F[A]): F[A] =
    MonadError_[F, E].ensuring(fa) {
      case Some(e) => report(Condition.abnormal(e))
      case None    => report(Condition.normal())
    }
}

object ConditionReportingDataSource {
  def apply[E, F[_]: Monad: MonadError_[?[_], E], G[_], Q, R](
      f: Condition[E] => F[Unit],
      ds: DataSource[F, G, Q, R])
      : DataSource[F, G, Q, R] =
    new ConditionReportingDataSource[E, F, G, Q, R](f, ds)
}
