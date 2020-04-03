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

package quasar.impl.datasource

import slamdata.Predef.{Boolean, Option, Unit}
import quasar.Condition
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.datasource.{Datasource, Loader}

import scala.util.{Left, Right}

import cats.{MonadError, ~>}
import cats.data.NonEmptyList
import cats.implicits._

final class ConditionReportingDatasource[
    E, F[_], G[_], Q, R, P <: ResourcePathType] private (
    report: Condition[E] => F[Unit],
    val underlying: Datasource[F, G, Q, R, P])(
    implicit ME: MonadError[F, E])
    extends Datasource[F, G, Q, R, P] {

  val kind: DatasourceType = underlying.kind

  lazy val loaders: NonEmptyList[Loader[F, Q, R]] =
    underlying.loaders.map(_.mapK(reportCondition))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    reportCondition(underlying.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    reportCondition(underlying.prefixedChildPaths(path))

  ////

  private val reportCondition: F ~> F =
    λ[F ~> F] { fa =>
      ME.attempt(fa) flatMap {
        case Left(e) =>
          report(Condition.abnormal(e)) >> ME.raiseError(e)

        case Right(a) =>
          report(Condition.normal()).as(a)
      }
    }
}

object ConditionReportingDatasource {
  def apply[E, F[_]: MonadError[?[_], E], G[_], Q, R, P <: ResourcePathType](
      f: Condition[E] => F[Unit],
      ds: Datasource[F, G, Q, R, P])
      : Datasource[F, G, Q, R, P] =
    new ConditionReportingDatasource[E, F, G, Q, R, P](f, ds)
}
