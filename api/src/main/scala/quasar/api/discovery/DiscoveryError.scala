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

package quasar.api.discovery

import quasar.api.resource.ResourcePath

import scala.{Nothing, Product, Serializable}

import cats.{Eq, Show}
import cats.implicits._

import monocle.Prism

import shims.{equalToCats, showToCats}

sealed trait DiscoveryError[+I] extends Product with Serializable

object DiscoveryError {

  final case class DatasourceNotFound[I](datasourceId: I)
      extends DiscoveryError[I]

  final case class PathNotFound(path: ResourcePath)
    extends DiscoveryError[Nothing]

  final case class PathNotAResource(path: ResourcePath)
    extends DiscoveryError[Nothing]

  def datasourceNotFound[I, E >: DatasourceNotFound[I] <: DiscoveryError[I]]
      : Prism[E, I] =
    Prism.partial[E, I] {
      case DatasourceNotFound(id) => id
    } (DatasourceNotFound(_))

  def pathNotAResource[I]: Prism[DiscoveryError[I], ResourcePath] =
    Prism.partial[DiscoveryError[I], ResourcePath] {
      case PathNotAResource(p) => p
    } (PathNotAResource(_))

  def pathNotFound[I]: Prism[DiscoveryError[I], ResourcePath] =
    Prism.partial[DiscoveryError[I], ResourcePath] {
      case PathNotFound(p) => p
    } (PathNotFound(_))

  implicit def discoveryErrorEq[I: Eq]: Eq[DiscoveryError[I]] =
    Eq by { de => (
      datasourceNotFound[I, DiscoveryError[I]].getOption(de),
      pathNotAResource[I].getOption(de),
      pathNotFound[I].getOption(de)
    )}

  implicit def discoveryErrorShow[I: Show]: Show[DiscoveryError[I]] =
    Show show {
      case DatasourceNotFound(i) =>
        "DatasourceNotFound(" + i.show + ")"

      case PathNotAResource(p) =>
        "PathNotAResource(" + p.show + ")"

      case PathNotFound(p) =>
        "PathNotFound(" + p.show + ")"
    }
}
