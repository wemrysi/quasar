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

import slamdata.Predef.{Boolean, None, Option, Some}

import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{Datasource, MonadResourceErr, PhysicalDatasource, ResourceError}
import quasar.contrib.scalaz._

import scala.util.{Left, Right}

import cats.Monad
import cats.syntax.applicative._
import cats.syntax.eq._
import cats.syntax.flatMap._
import cats.syntax.functor._
import cats.syntax.option._

import fs2.Stream

import monocle.Lens

import shims._

/** A datasource transformer that converts underlying prefix paths into prefix
  * resources by aggregating all child leaf resources of the prefix.
  */
final class ChildAggregatingDatasource[F[_]: Monad: MonadResourceErr, Q, R] private (
    underlying: PhysicalDatasource[F, Stream[F, ?], Q, R],
    queryPath: Lens[Q, ResourcePath])
    extends Datasource[F, Stream[F, ?], Q, CompositeResult[F, R]] {

  type PathType = ResourcePathType

  def kind: DatasourceType =
    underlying.kind

  def evaluate(q: Q): F[CompositeResult[F, R]] = {

    def aggregate(p: ResourcePath): F[AggregateResult[F, R]] =
      aggPrefixedChildPaths(p) flatMap {
        case Some(s) =>
          val agg =
            s.collect { case (n, ResourcePathType.LeafResource) => p / n }
              .evalMap(r => underlying.evaluate(queryPath.set(r)(q)).tupleLeft(r).attempt)
              .map(_.toOption)
              .unNone

          agg.pure[F]

        case None =>
          MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p))
      }

    val qpath = queryPath.get(q)

    underlying.pathIsResource(qpath).ifM(
      underlying.evaluate(q).map(Left(_)),
      aggregate(qpath).map(Right(_)))
  }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    underlying.pathIsResource(path).ifM(true.pure[F], aggPathExists(path))

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[Stream[F, (ResourceName, ResourcePathType)]]] =
    underlying.prefixedChildPaths(prefixPath)
      .map(_.map(s => Stream.emit((AggName, ResourcePathType.AggregateResource)) ++ s))

  ////

  private val AggName: ResourceName = ResourceName("*")

  private def aggPath(path: ResourcePath): Option[ResourcePath] =
    path.unsnoc.flatMap {
      case (p, n) => if (n === AggName) Some(p) else None
    }

  private def aggPathExists(path: ResourcePath): F[Boolean] =
    aggPath(path)
      .map(p => underlying.prefixedChildPaths(p).map(_.isDefined))
      .getOrElse(false.pure[F])

  private def aggPrefixedChildPaths(path: ResourcePath): F[Option[Stream[F, (ResourceName, ResourcePathType.Physical)]]] =
    aggPath(path).map(p => underlying.prefixedChildPaths(p)).getOrElse(none.pure[F])

}

object ChildAggregatingDatasource {
  def apply[F[_]: Monad: MonadResourceErr, Q, R](
      underlying: PhysicalDatasource[F, Stream[F, ?], Q, R],
      queryPath: Lens[Q, ResourcePath])
      : Datasource[F, Stream[F, ?], Q, CompositeResult[F, R]] =
    new ChildAggregatingDatasource(underlying, queryPath)
}
