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

package quasar.connector.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{DatasourceSpec, MonadResourceErr, ResourceError}
import quasar.contrib.scalaz.MonadError_

import cats.effect.IO

import eu.timepit.refined.auto._

import scalaz.IMap
import scalaz.std.list._

import shims._

object MapBasedDatasourceSpec extends DatasourceSpec[IO, List] {

  implicit val ioMonadResourceErr: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val datasource =
    MapBasedDatasource.pure[IO, List](
      DatasourceType("pure-test", 1L),
      IMap(
        ResourcePath.root() / ResourceName("a") / ResourceName("b") -> 1,
        ResourcePath.root() / ResourceName("a") / ResourceName("c") -> 2,
        ResourcePath.root() / ResourceName("d") -> 3))

  def nonExistentPath: ResourcePath =
    ResourcePath.root() / ResourceName("x") / ResourceName("y")

  def gatherMultiple[A](fga: List[A]): IO[List[A]] =
    IO.pure(fga)

  "evaluation" >> {
    "known resource returns result" >>* {
      datasource
        .evaluate(ResourcePath.root() / ResourceName("d"))
        .map(_ must_=== 3)
    }

    "known prefix errors with 'not a resource'" >>* {
      val pfx = ResourcePath.root() / ResourceName("a")

      MonadResourceErr[IO].attempt(datasource.evaluate(pfx)).map(_ must be_-\/.like {
        case ResourceError.NotAResource(p) => p must equal(pfx)
      })
    }

    "unknown path errors with 'not found'" >>* {
      MonadResourceErr[IO].attempt(datasource.evaluate(nonExistentPath)).map(_ must be_-\/.like {
        case ResourceError.PathNotFound(p) => p must equal(nonExistentPath)
      })
    }
  }
}
