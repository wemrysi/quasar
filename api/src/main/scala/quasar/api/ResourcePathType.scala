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

import slamdata.Predef.{Product, None, Serializable, Some, Unit}
import quasar.fp.ski.κ

import monocle.Prism
import scalaz.{Cord, Order, Show}
import scalaz.std.option._
import scalaz.syntax.show._

/** Describes what a `ResourcePath` refers to. */
trait ResourcePathType extends Product with Serializable

object ResourcePathType extends ResourcePathTypeInstances {
  case object ResourcePrefix extends ResourcePathType
  final case class Resource(formats: MediaTypes) extends ResourcePathType

  val resourcePrefix: Prism[ResourcePathType, Unit] =
    Prism.partial[ResourcePathType, Unit] {
      case ResourcePrefix => ()
    } (κ(ResourcePrefix))

  val resource: Prism[ResourcePathType, MediaTypes] =
    Prism.partial[ResourcePathType, MediaTypes] {
      case Resource(mts) => mts
    } (Resource)
}

sealed abstract class ResourcePathTypeInstances {
  implicit val order: Order[ResourcePathType] =
    Order.orderBy {
      case ResourcePathType.ResourcePrefix => None
      case ResourcePathType.Resource(mts) => Some(mts)
    }

  implicit val show: Show[ResourcePathType] =
    Show.show {
      case ResourcePathType.ResourcePrefix =>
        Cord("ResourcePrefix")

      case ResourcePathType.Resource(mts) =>
        Cord("Resource(") ++ mts.show ++ Cord(")")
    }
}
