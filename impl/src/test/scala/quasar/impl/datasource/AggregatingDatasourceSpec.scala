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
import quasar.api.resource._
import quasar.connector._
import quasar.connector.datasource._
import quasar.contrib.cats.monadError
import quasar.contrib.scalaz.MonadError_

import cats.MonadError
import cats.data.NonEmptyList
import cats.effect.{IO, Resource}

import eu.timepit.refined.auto._

import fs2.Stream

import monocle.Lens

import scalaz.IMap
import scalaz.std.either._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.equal._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._

import shims.monadToScalaz

object AggregatingDatasourceSpec extends DatasourceSpec[IO, Stream[IO, ?], ResourcePathType] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val ioCatsMonadResourceErr: MonadError[IO, ResourceError] =
    monadError.facet[IO](ResourceError.throwableP)

  val underlying =
    MapBasedDatasource.pure[Resource[IO, ?], Stream[IO, ?]](
      DatasourceType("pure-test", 1L),
      IMap(
        ResourcePath.root() / ResourceName("a") / ResourceName("b") -> 1,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") -> 2,
        ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("r") -> 3,
        ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("s") -> 4,
        ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("s") / ResourceName("t") -> 5,
        ResourcePath.root() / ResourceName("d") -> 6))

  val aggregating =
    AggregatingDatasource(underlying, Lens.id)

  val datasource =
    Resource.pure(aggregating)

  def nonExistentPath: ResourcePath =
    ResourcePath.root() / ResourceName("x") / ResourceName("y")

  def gatherMultiple[A](fga: Stream[IO, A]): IO[List[A]] =
    fga.compile.to(List)

  "aggregate discovery" >> {
    "underlying prefix resources are preserved" >>* {
      val z = ResourcePath.root() / ResourceName("z")
      val x = ResourceName("x")
      val y = ResourceName("y")

      val paths = IMap(z -> 1, (z / x) -> 2, (z / y) -> 3)

      val uds =
        new PhysicalDatasource[Resource[IO, ?], Stream[IO, ?], ResourcePath, Int] {
          val kind = DatasourceType("prefixed", 6L)

          val loaders = NonEmptyList.of(Loader.Batch(BatchLoader.Full { (rp: ResourcePath) =>
            Resource.pure[IO, Int](paths.lookup(rp) getOrElse -1)
          }))

          def pathIsResource(rp: ResourcePath): Resource[IO, Boolean] =
            Resource.pure(paths.member(rp))

          def prefixedChildPaths(rp: ResourcePath)
              : Resource[IO, Option[Stream[IO, (ResourceName, ResourcePathType.Physical)]]] =
            Resource pure {
              if (rp ≟ ResourcePath.root())
                Some(Stream(ResourceName("z") -> ResourcePathType.prefixResource))
              else if (rp ≟ z)
                Some(Stream(
                  ResourceName("x") -> ResourcePathType.leafResource,
                  ResourceName("y") -> ResourcePathType.leafResource))
              else if (paths.member(rp))
                Some(Stream.empty)
              else
                None
            }
        }

      val ds = AggregatingDatasource(uds, Lens.id)

      val result = for {
        dres <- ds.prefixedChildPaths(ResourcePath.root())
        meta <- Resource.liftF(dres.traverse(_.compile.to(List)))
        qres <- ds.loadFull(z).value
      } yield {
        meta must beSome(equal[List[(ResourceName, ResourcePathType)]](List(
          ResourceName("**") -> ResourcePathType.AggregateResource,
          ResourceName("z") -> ResourcePathType.prefixResource)))
        qres must beSome(beLeft(1))
      }

      result.use(IO.pure(_))
    }

    "agg resource is recognized via pathIsResource" >>* {
      aggregating
        .pathIsResource(ResourcePath.root() / ResourceName("a") / ResourceName("**"))
        .use(b => IO.pure(b must beTrue))
    }

    "children of prefix = agg resource + underlying children" >>* {
      aggregating
        .prefixedChildPaths(ResourcePath.root() / ResourceName("a"))
        .use(_.cata(_.compile.to(List), IO.pure(Nil)))
        .map(_ must equal[List[(ResourceName, ResourcePathType)]](List(
          ResourceName("**") -> ResourcePathType.AggregateResource,
          ResourceName("b") -> ResourcePathType.LeafResource,
          ResourceName("c") -> ResourcePathType.LeafResource,
          ResourceName("q") -> ResourcePathType.Prefix)))
    }
  }

  "evaluation" >> {
    "querying a non-existent path is not found" >>* {
      val result =
        aggregating
          .loadFull(nonExistentPath)
          .value
          .use(IO.pure(_))

      MonadResourceErr[IO].attempt(result).map(_ must be_-\/.like {
        case ResourceError.PathNotFound(p) => p must equal(nonExistentPath)
      })
    }

    "querying an underlying resource is unaffected" >>* {
      aggregating
        .loadFull(ResourcePath.root() / ResourceName("d"))
        .value
        .use(r => IO.pure(r must beSome(beLeft(6))))
    }

    "querying an agg resource aggregates descendant leafs" >>* {
      val b = ResourcePath.root() / ResourceName("a") / ResourceName("b")
      val c = ResourcePath.root() / ResourceName("a") / ResourceName("c")
      val r = ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("r")
      val s = ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("s")
      val t = ResourcePath.root() / ResourceName("a") / ResourceName("q") / ResourceName("s") / ResourceName("t")

      aggregating
        .loadFull(ResourcePath.root() / ResourceName("a") / ResourceName("**"))
        .semiflatMap(r => Resource.liftF(r.traverse(_.compile.to(List))))
        .value
        .use(x => IO.pure(x must beSome(beRight(contain(exactly((b, 1), (c, 2), (r, 3), (s, 4), (t, 5)))))))
    }
  }
}
