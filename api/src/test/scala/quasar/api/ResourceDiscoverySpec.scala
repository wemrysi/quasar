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

package quasar.api

import slamdata.Predef.String
import quasar.Qspec

import org.specs2.execute.AsResult
import org.specs2.matcher.Matcher
import org.specs2.specification.core.Fragment
import scalaz.{\/, ~>, Foldable, Id, Monad}, Id.Id
import scalaz.syntax.foldable._
import scalaz.syntax.monad._

abstract class ResourceDiscoverySpec[F[_]: Monad, G[_]: Foldable]
    extends Qspec {

  import ResourceError._

  def discovery: ResourceDiscovery[F, G]

  def nonExistentPath: ResourcePath

  def run: F ~> Id

  "ResourceDiscovery" >> {
    "must not be empty" >>* {
      discovery
        .children(ResourcePath.root())
        .map(_.any(g => !g.empty))
    }

    "children of non-existent is not found" >>* {
      discovery.children(nonExistentPath).map(_ must beNotFound(nonExistentPath))
    }

    "non-existent is not a resource" >>* {
      discovery.isResource(nonExistentPath).map(_ must beFalse)
    }

    "child status agrees with isResource" >>* {
      discovery
        .children(ResourcePath.root())
        .flatMap(_.anyM(_.allM {
          case (n, ResourcePathType.Resource) =>
            discovery.isResource(ResourcePath.root() / n)

          case (n, ResourcePathType.ResourcePrefix) =>
            discovery
              .isResource(ResourcePath.root() / n)
              .map(!_)
        }))
    }
  }

  ////

  val orG = Foldable[CommonError \/ ?].compose[G]

  def beNotFound[A](path: ResourcePath): Matcher[CommonError \/ A] =
    be_-\/(equal[ResourceError](PathNotFound(path)))

  implicit class RunExample(s: String) {
    def >>*[A: AsResult](fa: => F[A]): Fragment =
      s >> run(fa)
  }
}
