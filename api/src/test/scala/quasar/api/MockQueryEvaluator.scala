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

import slamdata.Predef.{tailrec, Boolean, None, Option, Some, Unit}
import quasar.api.ResourceError._
import quasar.fp.ski.κ

import scalaz.{\/, Applicative, IMap, Order, Tree}
import scalaz.std.stream._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.std.option._

final class MockQueryEvaluator[F[_]: Applicative, Q, R] private (
    resources: Tree[ResourceName],
    eval: Q => F[ReadError \/ R])
  extends QueryEvaluator[F, Q, R] {

  def children(path: ResourcePath): F[CommonError \/ IMap[ResourceName, ResourcePathType]] = {
    val progeny = subtreeAt(path) map { t =>
      IMap.fromFoldable(t.subForest map { k =>
        val typ =
          if (k.subForest.isEmpty) ResourcePathType.resource
          else ResourcePathType.resourcePrefix

        (k.rootLabel, typ)
      })
    }

    (progeny \/> (PathNotFound(path) : CommonError)).point[F]
  }

  def descendants(path: ResourcePath): F[CommonError \/ Tree[ResourceName]] =
    (subtreeAt(path) \/> (PathNotFound(path) : CommonError)).point[F]

  def isResource(path: ResourcePath): F[Boolean] =
    subtreeAt(path).exists(_.subForest.isEmpty).point[F]

  def evaluate(query: Q): F[ReadError \/ R] =
    eval(query)

  ////

  private def subtreeAt(path: ResourcePath): Option[Tree[ResourceName]] = {
    @tailrec
    def go(p: ResourcePath, t: Tree[ResourceName]): Option[Tree[ResourceName]] =
      path.uncons match {
        case Some((n, p1)) =>
          t.subForest.find(_.rootLabel === n) match {
            case Some(t0) => go(p1, t0)
            case None => None
          }

        case None => Some(t)
      }

    go(path, resources)
  }
}

object MockQueryEvaluator {
  def apply[F[_]: Applicative, Q, R](
      resources: Tree[ResourceName],
      eval: Q => F[ReadError \/ R])
      : QueryEvaluator[F, Q, R] =
    new MockQueryEvaluator(resources, eval)

  def fromResponseIMap[F[_]: Applicative, Q: Order, R](
      resources: Tree[ResourceName],
      responses: IMap[Q, R])
      : QueryEvaluator[F, Q, R] =
    apply(resources, q => (responses.lookup(q) \/> rootNotFound).point[F])

  def resourceDiscovery[F[_]: Applicative](
      resources: Tree[ResourceName])
      : ResourceDiscovery[F] =
    apply(resources, κ(rootNotFound.left[Unit].point[F]))

  ////

  private val rootNotFound: ReadError =
    PathNotFound(ResourcePath.root())
}
