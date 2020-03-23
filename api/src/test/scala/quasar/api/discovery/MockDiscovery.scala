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

import quasar.api.resource._

import scala.{Boolean, None, Option, Some}
import scala.annotation.tailrec
import scala.collection.immutable.{Stream => SStream, Set}
import scala.util.Either

import cats.{Applicative, MonoidK}
import cats.implicits._

import scalaz.Tree

import shims.equalToCats

final class MockDiscovery[F[_]: Applicative, G[_]: Applicative: MonoidK, I](
    extant: Set[I],
    structure: SStream[Tree[ResourceName]])
    extends Discovery[F, G, I, MockSchemaConfig.type] {

  import DiscoveryError._

  type PathType = ResourcePathType

  def pathIsResource(id: I, path: ResourcePath): F[Either[DatasourceNotFound[I], Boolean]] =
    Applicative[F] pure {
      if (extant(id))
        forestAt(path).exists(_.isEmpty).asRight[DatasourceNotFound[I]]
      else
        datasourceNotFound[I, DatasourceNotFound[I]](id).asLeft[Boolean]
    }

  def prefixedChildPaths(id: I, prefixPath: ResourcePath)
      : F[Either[DiscoveryError[I], G[(ResourceName, ResourcePathType)]]] =
    Applicative[F] pure {
      if (extant(id)) {
        val progeny =
          forestAt(prefixPath).map(_.foldMap(t =>
            (
              t.rootLabel,
              (if (t.subForest.isEmpty)
                ResourcePathType.leafResource
              else
                ResourcePathType.prefix): ResourcePathType
            ).pure[G])(MonoidK[G].algebra))

        progeny.toRight(pathNotFound[I](prefixPath))
      } else {
        datasourceNotFound[I, DiscoveryError[I]](id).asLeft[G[(ResourceName, ResourcePathType)]]
      }
    }

  def resourceSchema(
      id: I,
      path: ResourcePath,
      cfg: MockSchemaConfig.type)
      : F[Either[DiscoveryError[I], cfg.Schema]] =
    Applicative[F] pure {
      if (extant(id))
        forestAt(path) match {
          case Some(forest) if forest.isEmpty => MockSchemaConfig.MockSchema.asRight
          case Some(_) => pathNotAResource[I](path).asLeft
          case None => pathNotFound[I](path).asLeft
        }
      else
        datasourceNotFound[I, DiscoveryError[I]](id).asLeft
    }

  ////

  private def forestAt(path: ResourcePath): Option[SStream[Tree[ResourceName]]] = {
    @tailrec
    def go(p: ResourcePath, f: SStream[Tree[ResourceName]]): Option[SStream[Tree[ResourceName]]] =
      p.uncons match {
        case Some((n, p1)) =>
          f.find(_.rootLabel === n) match {
            case Some(f0) => go(p1, f0.subForest)
            case None => None
          }

        case None => Some(f)
      }

    go(path, structure)
  }
}
