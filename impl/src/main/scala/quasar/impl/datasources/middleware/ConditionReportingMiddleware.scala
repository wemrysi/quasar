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

package quasar.impl.datasources.middleware

import quasar.Condition
import quasar.api.resource.ResourcePathType
import quasar.connector.datasource.Datasource
import quasar.impl.QuasarDatasource
import quasar.impl.datasource.ConditionReportingDatasource

import scala.Unit

import cats.{MonadError, ~>}
import cats.effect.Resource
import cats.syntax.functor._

object ConditionReportingMiddleware {
  def apply[F[_], I, E](onChange: (I, Condition[E]) => F[Unit])
      : PartiallyApplied[F, I, E] =
    new PartiallyApplied(onChange)

  final class PartiallyApplied[F[_], I, E](onChange: (I, Condition[E]) => F[Unit]) {
    def apply[T[_[_]], G[_], R, P <: ResourcePathType](
        id: I, mds: QuasarDatasource[T, Resource[F, ?], G, R, P])(
        implicit
        F: MonadError[F, E])
        : F[QuasarDatasource[T, Resource[F, ?], G, R, P]] =
      onChange(id, Condition.normal()) as {
        mds.modify(λ[Datasource[Resource[F, ?], G, ?, R, P] ~> Datasource[Resource[F, ?], G, ?, R, P]] { ds =>
          ConditionReportingDatasource((c: Condition[E]) => Resource.liftF(onChange(id, c)), ds)
        })
      }
  }
}
