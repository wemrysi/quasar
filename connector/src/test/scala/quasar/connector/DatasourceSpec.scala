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

package quasar.connector

import slamdata.Predef.{Either, String, Throwable}
import quasar.Qspec
import quasar.common.resource._

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.{Effect, IO}
import fs2.async.Promise
import org.specs2.execute.AsResult
import org.specs2.specification.core.Fragment
import scalaz.Foldable
import scalaz.std.option._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import shims._

abstract class DatasourceSpec[F[_]: Effect, G[_]: Foldable, Q, R]
    extends Qspec {

  def datasource: Datasource[F, G, Q, R]

  def nonExistentPath: ResourcePath

  "resource discovery" >> {
    "must not be empty" >>* {
      datasource
        .prefixedChildPaths(ResourcePath.root())
        .map(_.any(g => !g.empty))
    }

    "children of non-existent is not found" >>* {
      datasource
        .prefixedChildPaths(nonExistentPath)
        .map(_ must beNone)
    }

    "non-existent is not a resource" >>* {
      datasource
        .pathIsResource(nonExistentPath)
        .map(_ must beFalse)
    }

    "prefixed child status agrees with pathIsResource" >>* {
      datasource
        .prefixedChildPaths(ResourcePath.root())
        .flatMap(_.anyM(_.allM {
          case (n, ResourcePathType.LeafResource | ResourcePathType.PrefixResource) =>
            datasource.pathIsResource(ResourcePath.root() / n)

          case (n, ResourcePathType.Prefix) =>
            datasource.pathIsResource(ResourcePath.root() / n).map(!_)
        }))
    }
  }

  ////

  implicit class RunExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Fragment = {
      val run = for {
        p <- Promise.empty[IO, Either[Throwable, A]]
        _ <- Effect[F].runAsync(fa)(p.complete(_))
        r <- p.get
        a <- IO.fromEither(r)
      } yield a

      s >> run.unsafeRunSync
    }
  }
}
