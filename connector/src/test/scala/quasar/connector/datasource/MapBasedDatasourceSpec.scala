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

package quasar.connector.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector._
import quasar.contrib.cats.monadError

import cats.MonadError
import cats.data.OptionT
import cats.effect.{IO, Resource}
import cats.instances.list._

import eu.timepit.refined.auto._

import scalaz.IMap

object MapBasedDatasourceSpec extends DatasourceSpec[IO, List, ResourcePathType.Physical] {

  implicit val ioMonadResourceErr: MonadError[IO, ResourceError] =
    monadError.facet[IO](ResourceError.throwableP)

  val mapDatasource =
    MapBasedDatasource.pure[Resource[IO, ?], List](
      DatasourceType("pure-test", 1L),
      IMap(
        ResourcePath.root() / ResourceName("a") -> 0,
        ResourcePath.root() / ResourceName("a") / ResourceName("b") -> 1,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") -> 2,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") / ResourceName("d") -> 3,
        ResourcePath.root() / ResourceName("d") / ResourceName("e") -> 4,
        ResourcePath.root() / ResourceName("f") -> 5))

  val datasource =
    Resource.pure(mapDatasource)

  def nonExistentPath: ResourcePath =
    ResourcePath.root() / ResourceName("x") / ResourceName("y")

  def gatherMultiple[A](fga: List[A]): IO[List[A]] =
    IO.pure(fga)

  def assertPrefixedChildPaths(path: ResourcePath, expected: Set[(ResourceName, ResourcePathType)]) =
    OptionT(mapDatasource.prefixedChildPaths(path))
      .getOrElseF(MonadError[Resource[IO, ?], Throwable].raiseError(
        new Exception(s"Failed to list resources under $path")))
      .use(r => IO.pure(r.toSet must_== expected))

  def full(path: ResourcePath): IO[Int] = {
    val loader = mapDatasource.loaders.toList collectFirst {
      case Loader.Batch(BatchLoader.Full(f)) => f
    }

    loader.fold(Resource.pure[IO, Int](-1))(_(path))
      .use(IO.pure(_))
  }


  "evaluation" >> {
    "known resource returns result" >>* {
      full(ResourcePath.root() / ResourceName("d") / ResourceName("e"))
        .map(_ must_=== 4)
    }

    "known prefix errors with 'not a resource'" >>* {
      val pfx = ResourcePath.root() / ResourceName("d")

      MonadError[IO, ResourceError].attempt(full(pfx)).map(_ must beLeft.like {
        case ResourceError.NotAResource(p) => p must equal(pfx)
      })
    }

    "unknown path errors with 'not found'" >>* {
      MonadError[IO, ResourceError].attempt(full(nonExistentPath)).map(_ must beLeft.like {
        case ResourceError.PathNotFound(p) => p must equal(nonExistentPath)
      })
    }

    "children of root is correct" >>* {
      assertPrefixedChildPaths(
        ResourcePath.root(),
        Set(
          ResourceName("a") -> ResourcePathType.prefixResource,
          ResourceName("d") -> ResourcePathType.prefix,
          ResourceName("f") -> ResourcePathType.leafResource))
    }

    "children of non-root is correct" >>* {
      assertPrefixedChildPaths(
        ResourcePath.root() / ResourceName("a"),
        Set(
          ResourceName("b") -> ResourcePathType.leafResource,
          ResourceName("c") -> ResourcePathType.prefixResource))
    }
  }
}
