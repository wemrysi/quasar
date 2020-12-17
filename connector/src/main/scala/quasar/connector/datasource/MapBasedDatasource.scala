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

package quasar.connector.datasource

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector._

import cats.{Applicative, ApplicativeError, MonadError, MonoidK}
import cats.data.NonEmptyList
import cats.syntax.applicative._
import cats.syntax.flatMap._

import scalaz.{ICons, IList, IMap, INil, ISet}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.foldable._

final class MapBasedDatasource[F[_], G[_]: Applicative, I, R] private (
    val kind: DatasourceType,
    content: IMap[ResourcePath, F[I]],
    loader: (ResourcePath => F[I]) => BatchLoader[F, ResourcePath, R])(
    implicit RE: ApplicativeError[F, ResourceError], G: MonoidK[G])
    extends PhysicalDatasource[F, G, ResourcePath, R] {

  import ResourceError._

  val loaders = NonEmptyList.of(Loader.Batch(loader { rp =>
    content.lookup(rp) getOrElse {
      RE.raiseError(
        if (prefixes.contains(rp)) notAResource(rp)
        else pathNotFound(rp))
    }
  }))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    content.member(path).pure[F]

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType.Physical)]]] =
    prefixedChildPaths0(prefixPath).pure[F]

  ////

  private val prefixes =
    content.keySet.foldLeft(ISet.empty[ResourcePath]) {
      case (acc, key) => acc union ancestors(key)
    }

  private def ancestors(p: ResourcePath): ISet[ResourcePath] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def go(ns: IList[ResourceName]): ISet[ResourcePath] = ns match {
      case INil() => ISet.singleton(ResourcePath.root())
      case l => ISet.singleton(ResourcePath.resourceNamesIso(l)) union go(l. dropRight(1))
    }

    go(ResourcePath.resourceNamesIso.get(p).dropRight(1))
  }

  private def children(s: ISet[ResourcePath], prefix: ResourcePath): ISet[ResourceName] =
    ISet.fromFoldable {
      s.toIList.flatMap(_.relativeTo(prefix).toIList).flatMap(p =>
        ResourcePath.resourceNamesIso.get(p) match {
          case ICons(h, INil()) => IList[ResourceName](h)
          case _ => INil[ResourceName]()
        })
    }

  private def prefixedChildPaths0(pfx: ResourcePath)
      : Option[G[(ResourceName, ResourcePathType.Physical)]] =
    if (!prefixes.contains(pfx)) {
      if (content.keySet.contains(pfx))
        Some(G.empty[(ResourceName, ResourcePathType.Physical)])
      else
        None
    } else {
      val childResources = children(content.keySet, pfx)
      val childPrefixes = children(prefixes, pfx)
      val both = childPrefixes intersection childResources

      val res = both.map((_, ResourcePathType.prefixResource)) union
        (childResources \\ both).map((_, ResourcePathType.leafResource)) union
        (childPrefixes \\ both).map((_, ResourcePathType.prefix))

      Some(res.foldLeft(G.empty[(ResourceName, ResourcePathType.Physical)]) {
        case (acc, p) => G.combineK(acc, p.pure[G])
      })
    }
}

object MapBasedDatasource {
  def apply[F[_], G[_]]: PartiallyApplied[F, G] =
    new PartiallyApplied[F, G]

  final class PartiallyApplied[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, F[R]])(
        implicit
        F: ApplicativeError[F, ResourceError],
        G0: Applicative[G],
        G1: MonoidK[G])
        : PhysicalDatasource[F, G, ResourcePath, R] =
      full[F, G](kind, content)
  }

  def pure[F[_], G[_]]: PartiallyAppliedPure[F, G] =
    new PartiallyAppliedPure[F, G]

  final class PartiallyAppliedPure[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, R])(
        implicit
        F: ApplicativeError[F, ResourceError],
        G0: Applicative[G],
        G1: MonoidK[G])
        : PhysicalDatasource[F, G, ResourcePath, R] =
      full[F, G](kind, content.map(_.pure[F]))
  }

  def full[F[_], G[_]]: PartiallyAppliedFull[F, G] =
    new PartiallyAppliedFull[F, G]

  final class PartiallyAppliedFull[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType, content: IMap[ResourcePath, F[R]])(
        implicit
        F: ApplicativeError[F, ResourceError],
        G0: Applicative[G],
        G1: MonoidK[G])
        : PhysicalDatasource[F, G, ResourcePath, R] = {

      val loader: (ResourcePath => F[R]) => BatchLoader[F, ResourcePath, R] =
        BatchLoader.Full(_)

      new MapBasedDatasource[F, G, R, R](kind, content, loader)
    }
  }

  def seek[F[_], G[_]]: PartiallyAppliedSeek[F, G] =
    new PartiallyAppliedSeek[F, G]

  final class PartiallyAppliedSeek[F[_], G[_]] {
    def apply[R](
        kind: DatasourceType,
        content: IMap[ResourcePath, F[Option[Offset] => F[R]]])(
        implicit
        F: MonadError[F, ResourceError],
        G0: Applicative[G],
        G1: MonoidK[G])
        : PhysicalDatasource[F, G, ResourcePath, R] = {

      val loader: (ResourcePath => F[Option[Offset] => F[R]]) => BatchLoader[F, ResourcePath, R] =
        f => BatchLoader.Seek((rp: ResourcePath, o: Option[Offset]) => f(rp).flatMap(_(o)))

      new MapBasedDatasource[F, G, Option[Offset] => F[R], R](kind, content, loader)
    }
  }
}
