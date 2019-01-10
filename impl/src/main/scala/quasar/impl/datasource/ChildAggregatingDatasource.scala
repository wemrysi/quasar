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

import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{Datasource, MonadResourceErr, ResourceError}
import quasar.contrib.matryoshka.totally
import quasar.contrib.scalaz._

import scala.util.{Either, Left, Right}

import cats.{Functor, Monad}
import cats.instances.option._
import cats.syntax.applicative._
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.Stream

import monocle.Lens

import shims._

/** A datasource transformer that converts underlying prefix paths into prefix
  * resources by aggregating all child leaf resources of the prefix.
  */
final class ChildAggregatingDatasource[F[_]: Monad: MonadResourceErr, Q, R, S, T] private (
    underlying: Datasource[F, Stream[F, ?], Q, R],
    queryPath: Q => ResourcePath,
    componentQuery: (Q, ResourcePath) => (Q, S),
    componentResult: (R, S) => T)
    extends Datasource[F, Stream[F, ?], Q, Either[R, AggregateResult[F, T]]] {

  def kind: DatasourceType =
    underlying.kind

  def evaluate(q: Q): F[Either[R, AggregateResult[F, T]]] = {
    def aggregate(p: ResourcePath): F[AggregateResult[F, T]] =
      underlying.prefixedChildPaths(p) flatMap {
        case Some(s) =>
          val resources =
            s collect {
              case (n, ResourcePathType.LeafResource) => p / n
            }

          val results =
            resources evalMap { cp =>
              val (cq, s) = componentQuery(q, cp)
              underlying.evaluate(cq).map(r => (cp, componentResult(r, s))).attempt
            }

          results.map(_.toOption).unNone.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p))
      }

    val qpath = queryPath(q)

    underlying.pathIsResource(qpath).ifM(
      underlying.evaluate(q).map(Left(_)),
      aggregate(qpath).map(Right(_)))
  }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    underlying.pathIsResource(path).ifM(true.pure[F], pathExists(path))

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] =
    FOS.map(underlying.prefixedChildPaths(prefixPath))(totally {
      case (n, ResourcePathType.Prefix) => (n, ResourcePathType.PrefixResource)
    })

  ////

  private val FOS = Functor[F].compose[Option].compose[Stream[F, ?]]

  private def pathExists(path: ResourcePath): F[Boolean] =
    underlying.prefixedChildPaths(path).map(_.isDefined)
}

object ChildAggregatingDatasource {
  def apply[F[_]: Monad: MonadResourceErr, Q, R, S, T](
      underlying: Datasource[F, Stream[F, ?], Q, R])(
      queryPath: Q => ResourcePath,
      componentQuery: (Q, ResourcePath) => (Q, S),
      componentResult: (R, S) => T)
      : Datasource[F, Stream[F, ?], Q, Either[R, AggregateResult[F, T]]] =
    new ChildAggregatingDatasource(underlying, queryPath, componentQuery, componentResult)

  def composite[F[_]: Monad: MonadResourceErr, Q, R](
      underlying: Datasource[F, Stream[F, ?], Q, R],
      queryPath: Lens[Q, ResourcePath])
      : Datasource[F, Stream[F, ?], Q, CompositeResult[F, R]] =
    apply(underlying)(queryPath.get, (q, p) => (queryPath.set(p)(q), ()), (r, _: Unit) => r)
}
