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

import slamdata.Predef.List
import quasar.EffectfulQSpec
import quasar.api.resource._

import scala.concurrent.ExecutionContext.Implicits.global

import cats.effect.Effect
import scalaz.Scalaz._
import shims._

abstract class DatasourceSpec[F[_]: Effect, G[_]]
    extends EffectfulQSpec[F] {

  def datasource: Datasource[F, G, _]

  def nonExistentPath: ResourcePath

  def gatherMultiple[A](fga: G[A]): F[List[A]]

  "resource discovery" >> {
    "must not be empty" >>* {
      datasource
        .prefixedChildPaths(ResourcePath.root())
        .flatMap(_.traverse(gatherMultiple))
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
        .flatMap(_.traverse(gatherMultiple))
        .flatMap(_.anyM(_.allM {
          case (n, ResourcePathType.LeafResource | ResourcePathType.PrefixResource) =>
            datasource.pathIsResource(ResourcePath.root() / n)

          case (n, ResourcePathType.Prefix) =>
            datasource.pathIsResource(ResourcePath.root() / n).map(!_)
        }))
    }
  }
}
