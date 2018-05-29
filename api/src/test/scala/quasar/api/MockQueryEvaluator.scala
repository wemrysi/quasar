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

import slamdata.Predef.{tailrec, Boolean, None, Option, Some, Stream, Unit}
import quasar.api.ResourceError._
import quasar.fp.ski.κ

import scalaz.{\/, Applicative, ApplicativePlus, IMap, Order, Tree}
import scalaz.std.stream._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.plusEmpty._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

final class MockQueryEvaluator[F[_]: Applicative, G[_]: ApplicativePlus, Q, R] private (
    resources: Stream[Tree[ResourceName]],
    eval: Q => F[ReadError \/ R])
  extends QueryEvaluator[F, G, Q, R] {

  def children(path: ResourcePath): F[CommonError \/ G[(ResourceName, ResourcePathType)]] = {
    val progeny =
      forestAt(path).map(_.foldMap(t =>
        (
          t.rootLabel,
          t.subForest.isEmpty.fold(
            ResourcePathType.resource,
            ResourcePathType.resourcePrefix)
        ).point[G])(ApplicativePlus[G].monoid))

    (progeny \/> pathNotFound[CommonError](path)).point[F]
  }

  def descendants(path: ResourcePath): F[CommonError \/ G[ResourcePath]] = {
    def pathify(f: Stream[Tree[ResourceName]], prefixes: G[ResourcePath]): G[ResourcePath] =
      if (f.isEmpty)
        prefixes
      else
        f.foldMap(t => pathify(t.subForest, prefixes.map(_ / t.rootLabel)))(ApplicativePlus[G].monoid)

    forestAt(path)
      .map(f => f.isEmpty.fold(mempty[G, ResourcePath], pathify(f, path.point[G])))
      .toRightDisjunction(pathNotFound[CommonError](path))
      .point[F]
  }

  def isResource(path: ResourcePath): F[Boolean] =
    forestAt(path).exists(_.isEmpty).point[F]

  def evaluate(query: Q): F[ReadError \/ R] =
    eval(query)

  ////

  private def forestAt(path: ResourcePath): Option[Stream[Tree[ResourceName]]] = {
    @tailrec
    def go(p: ResourcePath, f: Stream[Tree[ResourceName]]): Option[Stream[Tree[ResourceName]]] =
      p.uncons match {
        case Some((n, p1)) =>
          f.find(_.rootLabel === n) match {
            case Some(f0) => go(p1, f0.subForest)
            case None => None
          }

        case None => Some(f)
      }

    go(path, resources)
  }
}

object MockQueryEvaluator {
  def apply[F[_]: Applicative, G[_]: ApplicativePlus, Q, R](
      resources: Stream[Tree[ResourceName]],
      eval: Q => F[ReadError \/ R])
      : QueryEvaluator[F, G, Q, R] =
    new MockQueryEvaluator(resources, eval)

  def fromResponseIMap[F[_]: Applicative, G[_]: ApplicativePlus, Q: Order, R](
      resources: Stream[Tree[ResourceName]],
      responses: IMap[Q, R])
      : QueryEvaluator[F, G, Q, R] =
    apply(resources, q => (responses.lookup(q) \/> rootNotFound).point[F])

  def resourceDiscovery[F[_]: Applicative, G[_]: ApplicativePlus](
      resources: Stream[Tree[ResourceName]])
      : ResourceDiscovery[F, G] =
    apply(resources, κ(rootNotFound.left[Unit].point[F]))

  ////

  private val rootNotFound: ReadError =
    PathNotFound(ResourcePath.root())
}
