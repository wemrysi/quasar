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

package quasar.impl.datasource

import slamdata.Predef.{Boolean, Option}

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.{Datasource, MonadResourceErr, ResourceError}

import scalaz.{Applicative, ApplicativePlus, IMap, Scalaz}, Scalaz._

final class PureDatasource[F[_]: Applicative: MonadResourceErr, G[_]: ApplicativePlus, R] private (
    val kind: DatasourceType,
    content: IMap[ResourcePath, R])
    extends Datasource[F, G, ResourcePath, R] {

  import ResourceError._

  def evaluate(rp: ResourcePath): F[R] =
    if (prefixedChildPaths0(rp).isDefined)
      content.lookup(rp) getOrElseF {
        MonadResourceErr[F].raiseError(notAResource(rp))
      }
    else
      MonadResourceErr[F].raiseError(pathNotFound(rp))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    content.member(path).point[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]] =
    prefixedChildPaths0(prefixPath).point[F]

  ////

  private def prefixedChildPaths0(pfx: ResourcePath)
      : Option[G[(ResourceName, ResourcePathType)]] =
    if (content.member(pfx)) {
      some(mempty[G, (ResourceName, ResourcePathType)])
    } else {
      val children =
        content.keys
          .flatMap(_.relativeTo(pfx).toList)
          .toNel

      val results =
        children.flatMap(_.traverse(_.uncons map {
          case (n, _) =>
            val tpe =
              if (content.member(pfx / n))
                ResourcePathType.leafResource
              else
                ResourcePathType.prefix

            (n, tpe)
        }))

      results.map(_.collapse[G])
    }
}

object PureDatasource {
  def apply[F[_], G[_]]: PartiallyApplied[F, G] =
    new PartiallyApplied[F, G]

  final class PartiallyApplied[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, R])(
        implicit
        F0: Applicative[F],
        F1: MonadResourceErr[F],
        G0: ApplicativePlus[G])
        : Datasource[F, G, ResourcePath, R] =
      new PureDatasource[F, G, R](kind, content)
  }
}
