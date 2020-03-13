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

import slamdata.Predef.{Array, Boolean, Option, Some, SuppressWarnings}
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Offset

import cats.Applicative
import cats.data.{NonEmptyList, OptionT}
import cats.instances.option._
import cats.syntax.traverse._

import monocle.{PTraversal, Traversal}

import scalaz.syntax.functor._

import shims.applicativeToCats

/** @tparam F effects
  * @tparam G multiple results
  * @tparam Q query
  */
trait Datasource[F[_], G[_], Q, R, P <: ResourcePathType] {

  /** The type of this datasource. */
  def kind: DatasourceType

  /** The set of `Loader`s provided by this datasource. */
  def loaders: NonEmptyList[Loader[F, Q, R]]

  /** Returns whether or not the specified path refers to a resource in the
    * specified datasource.
    */
  def pathIsResource(path: ResourcePath): F[Boolean]

  /** Returns the name and type of the `ResourcePath`s implied by concatenating
    * each name to `prefixPath` or `None` if `prefixPath` does not exist.
    */
  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, P)]]]

  /** Attempts a 'full' load, returning `None` if unsupported by this datasource. */
  def loadFull(q: Q)(implicit F: Applicative[F]): OptionT[F, R] =
    OptionT {
      loaders
        .toList
        .collectFirst { case Loader.Batch(b) => b }
        .traverse(_.loadFull(q))
    }

  /** Attempts to seek and load from the supplied offset, returning `None`
    * if unsupported by this datasource.
    */
  def loadFrom(q: Q, offset: Offset)(implicit F: Applicative[F]): OptionT[F, R] =
    OptionT {
      loaders
        .toList
        .collectFirst { case Loader.Batch(BatchLoader.Seek(f)) => f }
        .traverse(_(q, Some(offset)))
    }
}

object Datasource {
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def widenPathType[F[_], G[_], Q, R, PI <: ResourcePathType, PO >: PI <: ResourcePathType](
      ds: Datasource[F, G, Q, R, PI])
      : Datasource[F, G, Q, R, PO] =
    ds.asInstanceOf[Datasource[F, G, Q, R, PO]]

  def loaders[F[_], G[_], Q, R, P <: ResourcePathType]
      : Traversal[Datasource[F, G, Q, R ,P], Loader[F, Q, R]] =
    ploaders[F, G, Q, R, Q, R, P]

  def ploaders[F[_], G[_], Q1, R1, Q2, R2, P <: ResourcePathType]
      : PTraversal[Datasource[F, G, Q1, R1, P], Datasource[F, G, Q2, R2, P], Loader[F, Q1, R1], Loader[F, Q2, R2]] =
    new PTraversal[Datasource[F, G, Q1, R1, P], Datasource[F, G, Q2, R2, P], Loader[F, Q1, R1], Loader[F, Q2, R2]] {
      def modifyF[H[_]: scalaz.Applicative](f: Loader[F, Q1, R1] => H[Loader[F, Q2, R2]])(s: Datasource[F, G, Q1, R1, P]) =
        s.loaders.traverse(f) map { ls =>
          new Datasource[F, G, Q2, R2, P] {
            val kind = s.kind
            val loaders = ls
            def pathIsResource(p: ResourcePath) = s.pathIsResource(p)
            def prefixedChildPaths(pfx: ResourcePath) = s.prefixedChildPaths(pfx)
          }
        }
    }
}
