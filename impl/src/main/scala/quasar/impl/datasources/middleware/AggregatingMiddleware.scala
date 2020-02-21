/*
 * Copyright 2014–2020 SlamData Inc.
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

import quasar.api.resource.{ResourcePath, ResourcePathType}
import quasar.connector.MonadResourceErr
import quasar.connector.datasource.Datasource
import quasar.impl.datasource.{AggregateResult, AggregatingDatasource, MonadCreateErr}
import quasar.qscript.{InterpretedRead, QScriptEducated}

import scala.util.{Either, Left}

import cats.Monad
import cats.effect.Sync
import cats.syntax.functor._

import fs2.Stream

import shims.{functorToCats, functorToScalaz}

object AggregatingMiddleware {
  def apply[T[_[_]], F[_]: MonadResourceErr: MonadCreateErr: Sync, I, R](
      datasourceId: I,
      mds: QuasarDatasource[T, F, Stream[F, ?], R, ResourcePathType.Physical])
      : F[QuasarDatasource[T, F, Stream[F, ?], Either[R, AggregateResult[F, R]], ResourcePathType]] =
    Monad[F].pure(mds) map {
      case QuasarDatasource.Lightweight(lw) =>
        val ds: Datasource[F, Stream[F, ?], InterpretedRead[ResourcePath], R, ResourcePathType.Physical] = lw
        QuasarDatasource.lightweight[T](AggregatingDatasource(ds, InterpretedRead.path))

      // TODO: union all in QScript?
      case QuasarDatasource.Heavyweight(hw) =>
        type Q = T[QScriptEducated[T, ?]]
        val ds: Datasource[F, Stream[F, ?], Q, Either[R, AggregateResult[F, R]], ResourcePathType.Physical] =
          Datasource.ploaders[F, Stream[F, ?], Q, R, Q, Either[R, AggregateResult[F, R]], ResourcePathType.Physical]
            .modify(_.map(Left(_)))(hw)
        QuasarDatasource.heavyweight(Datasource.widenPathType(ds))
    }
}
