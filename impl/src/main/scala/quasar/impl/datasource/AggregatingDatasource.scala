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

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.api.resource.{ResourceName, ResourcePath, ResourcePathType}
import quasar.connector.{MonadResourceErr, ResourceError}
import quasar.connector.datasource._

import scala.util.{Left, Right}

import cats.data.{NonEmptyList, OptionT}
import cats.effect.{Resource, Sync}
import cats.implicits._

import fs2.{Pull, Stream}

import monocle.Lens

import shims.equalToCats

/** A datasource transformer that augments underlying datasources by adding an aggregate resource
  * `p / **` for every prefix `p`. An aggregate resource `p / **` will aggregate
  * all descendant resources of the prefix `p`.
  */
final class AggregatingDatasource[F[_]: MonadResourceErr: Sync, Q, R] private(
    underlying: Datasource[Resource[F, ?], Stream[F, ?], Q, R, ResourcePathType.Physical],
    queryPath: Lens[Q, ResourcePath])
    extends Datasource[Resource[F, ?], Stream[F, ?], Q, CompositeResult[F, R], ResourcePathType] {

  def kind: DatasourceType =
    underlying.kind

  lazy val loaders: NonEmptyList[Loader[Resource[F, ?], Q, CompositeResult[F, R]]] =
    underlying.loaders map {
      case Loader.Batch(BatchLoader.Full(full)) =>
        Loader.Batch(BatchLoader.Full(aggregateFull(full)))

      case Loader.Batch(BatchLoader.Seek(seek)) =>
        Loader.Batch(BatchLoader.Seek {
          case (q, Some(o)) =>
            seek(q, Some(o)).map(Left(_))

          case (q, None) =>
            aggregateFull(seek(_, None))(q)
        })
    }

  def pathIsResource(path: ResourcePath): Resource[F, Boolean] = {
    // `p / **` exists iff `p` is prefix/prefixresource
    def aggPathExists: Resource[F, Boolean] = {
      // whether the provided stream emits any values
      // warning: runs the stream to completion
      def isNonEmpty[A](s: Stream[F, A]): F[Boolean] =
        s.pull.unconsNonEmpty
          .flatMap(_.fold(Pull.output1(false))(_ => Pull.output1(true)))
          .stream
          .compile
          .fold(false)(_ || _)

      OptionT.fromOption[Resource[F, ?]](aggPath(path))
        .flatMap(p => OptionT(underlying.prefixedChildPaths(p).evalMap(_.traverse(isNonEmpty))))
        .getOrElse(false)
    }

    underlying.pathIsResource(path).ifM(
      ifTrue = true.pure[Resource[F, ?]],
      ifFalse = aggPathExists)
  }

  def prefixedChildPaths(prefixPath: ResourcePath)
      : Resource[F, Option[Stream[F, (ResourceName, ResourcePathType)]]] =
    if (aggPath(prefixPath).isDefined)
      pathIsResource(prefixPath).ifM(
        ifTrue = Stream.empty.covary[F].covaryOutput[(ResourceName, ResourcePathType)].some.pure[Resource[F, ?]],
        ifFalse = none.pure[Resource[F, ?]])
    else
      underlying.prefixedChildPaths(prefixPath).map(_.map(
        _.pull.peek1.flatMap {
          case Some((_, s)) => Pull.output1(AggEntry) >> s.pull.echo
          case None => Pull.done
        }.stream))

  ////

  private val AggName: ResourceName = ResourceName("**")

  private val AggEntry: (ResourceName, ResourcePathType) =
    (AggName, ResourcePathType.AggregateResource)

  private def aggPath(path: ResourcePath): Option[ResourcePath] =
    path.unsnoc.flatMap {
      case (p, n) => if (n === AggName) Some(p) else None
    }

  private def aggregateFull(full: Q => Resource[F, R])(q: Q): Resource[F, CompositeResult[F, R]] = {
    def aggregate(p: ResourcePath): Resource[F, AggregateResult[F, R]] =
      aggPath(p)
        .map(doAggregate)
        .getOrElse(Resource.liftF(MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p))))

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def doAggregate(p: ResourcePath): Resource[F, AggregateResult[F, R]] =
      underlying.prefixedChildPaths(p) flatMap {
        case Some(s) =>
          val children: AggregateResult[F, R] =
            s.collect {
              case (n, ResourcePathType.LeafResource) => p / n
              case (n, ResourcePathType.PrefixResource) => p / n
            } .flatMap(r => Stream.resource(full(queryPath.set(r)(q)).tupleLeft(r).attempt))
              .map(_.toOption)
              .unNone

          val nested: AggregateResult[F, R] =
            s.collect {
              case (n, ResourcePathType.Prefix) => p / n
              case (n, ResourcePathType.PrefixResource) => p / n
            } .flatMap(r => Stream.resource(doAggregate(r))).flatten

          (children ++ nested).pure[Resource[F, ?]]

        case None =>
          Resource.liftF(MonadResourceErr[F].raiseError(ResourceError.pathNotFound(p)))
      }

    val qpath = queryPath.get(q)

    underlying.pathIsResource(qpath).ifM(
      full(q).map(Left(_)),
      aggregate(qpath).map(Right(_)))
  }
}

object AggregatingDatasource {
  def apply[F[_]: MonadResourceErr: Sync, Q, R](
      underlying: Datasource[Resource[F, ?], Stream[F, ?], Q, R, ResourcePathType.Physical],
      queryPath: Lens[Q, ResourcePath])
      : Datasource[Resource[F, ?], Stream[F, ?], Q, CompositeResult[F, R], ResourcePathType] =
    new AggregatingDatasource(underlying, queryPath)
}
