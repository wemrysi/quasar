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

package quasar.impl
package datasources.middleware

import quasar.connector.{Datasource, MonadResourceErr}
import quasar.impl.datasource.{AggregateResult, ChildAggregatingDatasource, MonadCreateErr}
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.{InterpretedRead, QScriptEducated}

import scala.util.{Either, Left}

import cats.Monad
import cats.effect.Sync
import cats.syntax.functor._
import fs2.Stream
import shims._

object ChildAggregatingMiddleware {
  def apply[T[_[_]], F[_]: MonadResourceErr: MonadCreateErr: Sync, I, R](
      datasourceId: I,
      mds: ManagedDatasource[T, F, Stream[F, ?], R])
      : F[ManagedDatasource[T, F, Stream[F, ?], Either[R, AggregateResult[F, R]]]] =
    Monad[F].pure(mds) map {
      case ManagedDatasource.ManagedLightweight(lw) =>
        ManagedDatasource.lightweight[T](
          ChildAggregatingDatasource(datasource.toPhysicalDatasource(lw), InterpretedRead.path))

      // TODO: union all in QScript?
      case ManagedDatasource.ManagedHeavyweight(hw) =>
        type Q = T[QScriptEducated[T, ?]]
        ManagedDatasource.heavyweight(
          Datasource.pevaluator[F, Stream[F, ?], Q, R, Q, Either[R, AggregateResult[F, R]]]
            .modify(_.map(Left(_)))(datasource.toPhysicalDatasource(hw)))
    }
}
