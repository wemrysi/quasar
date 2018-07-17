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

package quasar.common.resource

import slamdata.Predef.{Product, Serializable}

import monocle.Prism
import scalaz.{Cord, Equal, Show}
import scalaz.syntax.show._
import scalaz.std.option._
import scalaz.std.tuple._

sealed trait ResourceError extends Product with Serializable

object ResourceError extends ResourceErrorInstances{
  final case class NotAResource(path: ResourcePath) extends ResourceError
  sealed trait ExistentialError extends ResourceError
  final case class PathNotFound(path: ResourcePath) extends ExistentialError

  val notAResource: Prism[ResourceError, ResourcePath] =
    Prism.partial[ResourceError, ResourcePath] {
      case NotAResource(p) => p
    } (NotAResource(_))

  def pathNotFound[E >: ExistentialError <: ResourceError]: Prism[E, ResourcePath] =
    Prism.partial[E, ResourcePath] {
      case PathNotFound(p) => p
    } (PathNotFound(_))
}

sealed abstract class ResourceErrorInstances {
  implicit val equal: Equal[ResourceError] =
    Equal.equalBy(e => (
      ResourceError.notAResource.getOption(e),
      ResourceError.pathNotFound.getOption(e)))

  implicit val show: Show[ResourceError] =
    Show.show {
      case ResourceError.NotAResource(p) =>
        Cord("NotAResource(") ++ p.show ++ Cord(")")

      case ResourceError.PathNotFound(p) =>
        Cord("PathNotFound(") ++ p.show ++ Cord(")")
    }
}
