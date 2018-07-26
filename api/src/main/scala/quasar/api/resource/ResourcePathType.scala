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

package quasar.api.resource

import slamdata.Predef.{Boolean, Int, Product, Serializable, Some}

import scalaz.{Enum, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

/** The sorts of paths within a Datasource. */
sealed trait ResourcePathType extends Product with Serializable {
  def isPrefix: Boolean =
    this match {
      case ResourcePathType.Prefix => true
      case ResourcePathType.PrefixResource => true
      case ResourcePathType.LeafResource => false
    }

  def isResource: Boolean =
    this match {
      case ResourcePathType.Prefix => false
      case ResourcePathType.PrefixResource => true
      case ResourcePathType.LeafResource => true
    }
}

object ResourcePathType extends ResourcePathTypeInstances {
  /** The path does not refer to a resource, but is a prefix of one or more paths. */
  case object Prefix extends ResourcePathType
  /** The path refers to a resource and is a prefix of one or more paths. */
  case object PrefixResource extends ResourcePathType
  /** The path refers to a resource and is not a prefix of any other paths. */
  case object LeafResource extends ResourcePathType

  val leafResource: ResourcePathType =
    LeafResource

  val prefixResource: ResourcePathType =
    PrefixResource

  val prefix: ResourcePathType =
    Prefix
}

sealed abstract class ResourcePathTypeInstances {
  implicit val enum: Enum[ResourcePathType] =
    new Enum[ResourcePathType] {
      def order(x: ResourcePathType, y: ResourcePathType) =
        toInt(x) ?|? toInt(y)

      def pred(t: ResourcePathType) =
        t match {
          case ResourcePathType.Prefix => ResourcePathType.LeafResource
          case ResourcePathType.PrefixResource => ResourcePathType.Prefix
          case ResourcePathType.LeafResource => ResourcePathType.PrefixResource
        }

      def succ(t: ResourcePathType) =
        t match {
          case ResourcePathType.Prefix => ResourcePathType.PrefixResource
          case ResourcePathType.PrefixResource => ResourcePathType.LeafResource
          case ResourcePathType.LeafResource => ResourcePathType.Prefix
        }

      override val min = Some(ResourcePathType.Prefix)

      override val max = Some(ResourcePathType.LeafResource)

      private def toInt(t: ResourcePathType): Int =
        t match {
          case ResourcePathType.Prefix => 0
          case ResourcePathType.PrefixResource => 1
          case ResourcePathType.LeafResource => 2
        }
    }

  implicit val show: Show[ResourcePathType] =
    Show.showFromToString
}
