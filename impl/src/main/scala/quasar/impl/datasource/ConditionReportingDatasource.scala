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

import slamdata.Predef.{Boolean, None, Option, Some, Unit}
import quasar.Condition
import quasar.api.QueryEvaluator
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_

import qdata.QDataEncode
import scalaz.Monad

final class ConditionReportingDatasource[
    E, F[_]: Monad: MonadError_[?[_], E], G[_], Q] private (
    report: Condition[E] => F[Unit],
    underlying: Datasource[F, G, Q])
    extends Datasource[F, G, Q] {

  def evaluator[R: QDataEncode]: QueryEvaluator[F, Q, G[R]] =
    new QueryEvaluator[F, Q, G[R]] {
      def evaluate(query: Q): F[G[R]] =
        reportCondition(underlying.evaluator.evaluate(query))
    }

  val kind: DatasourceType = underlying.kind

  def pathIsResource(path: ResourcePath): F[Boolean] =
    reportCondition(underlying.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]] =
    reportCondition(underlying.prefixedChildPaths(path))

  ////

  private def reportCondition[A](fa: F[A]): F[A] =
    MonadError_[F, E].ensuring(fa) {
      case Some(e) => report(Condition.abnormal(e))
      case None    => report(Condition.normal())
    }
}

object ConditionReportingDatasource {
  def apply[E, F[_]: Monad: MonadError_[?[_], E], G[_], Q](
      f: Condition[E] => F[Unit],
      ds: Datasource[F, G, Q])
      : Datasource[F, G, Q] =
    new ConditionReportingDatasource[E, F, G, Q](f, ds)
}
