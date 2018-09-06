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

import slamdata.Predef.{Exception, Product, Serializable, String, StringContext, Throwable}

import quasar.api.resource._

import monocle.Prism
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.{Cord, Equal, Show}

sealed trait ResourceError extends Product with Serializable

object ResourceError extends ResourceErrorInstances{
  final case class MalformedResource(path: ResourcePath, expectedFormat: String, msg: String) extends ResourceError
  final case class NotAResource(path: ResourcePath) extends ResourceError
  sealed trait ExistentialError extends ResourceError
  final case class PathNotFound(path: ResourcePath) extends ExistentialError

  val malformedResource: Prism[ResourceError, (ResourcePath, String, String)] =
    Prism.partial[ResourceError, (ResourcePath, String, String)] {
      case MalformedResource(rp, expected, msg) => (rp, expected, msg)
    } (MalformedResource.tupled)

  val notAResource: Prism[ResourceError, ResourcePath] =
    Prism.partial[ResourceError, ResourcePath] {
      case NotAResource(p) => p
    } (NotAResource(_))

  def pathNotFound[E >: ExistentialError <: ResourceError]: Prism[E, ResourcePath] =
    Prism.partial[E, ResourcePath] {
      case PathNotFound(p) => p
    } (PathNotFound(_))

  val throwableP: Prism[Throwable, ResourceError] =
    Prism.partial[Throwable, ResourceError] {
      case ResourceErrorException(re) => re
    } (ResourceErrorException(_))

  ////

  private final case class ResourceErrorException(err: ResourceError)
      extends Exception(err.shows)
}

sealed abstract class ResourceErrorInstances {
  implicit val equal: Equal[ResourceError] = {
    Equal.equalBy(e => (
      ResourceError.notAResource.getOption(e),
      ResourceError.pathNotFound.getOption(e),
      ResourceError.malformedResource.getOption(e)))
  }

  implicit val show: Show[ResourceError] =
    Show.show {
      case ResourceError.NotAResource(p) =>
        Cord("NotAResource(") ++ p.show ++ Cord(")")

      case ResourceError.PathNotFound(p) =>
        Cord("PathNotFound(") ++ p.show ++ Cord(")")

      case ResourceError.MalformedResource(p, expected, msg) =>
        Cord(s"MalformedResource(path: ${p.show}, expected: $expected, msg: $msg)")
    }
}
