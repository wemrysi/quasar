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

package quasar.impl

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.datasource.Datasource
import quasar.qscript.{InterpretedRead, QScriptEducated}

import cats.~>

sealed trait QuasarDatasource[T[_[_]], F[_], G[_], R, P <: ResourcePathType] {
  import QuasarDatasource._

  def kind: DatasourceType =
    this match {
      case Lightweight(lw) => lw.kind
      case Heavyweight(hw) => hw.kind
    }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    this match {
      case Lightweight(lw) => lw.pathIsResource(path)
      case Heavyweight(hw) => hw.pathIsResource(path)
    }

  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, P)]]] =
    this match {
      case Lightweight(lw) => lw.prefixedChildPaths(prefixPath)
      case Heavyweight(hw) => hw.prefixedChildPaths(prefixPath)
    }

  def modify[V[_], W[_], S](
      f: Datasource[F, G, ?, R, P] ~> Datasource[V, W, ?, S, P])
      : QuasarDatasource[T, V, W, S, P] =
    this match {
      case Lightweight(lw) => Lightweight(f(lw))
      case Heavyweight(hw) => Heavyweight(f(hw))
    }
}

object QuasarDatasource {
  final case class Lightweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      lw: Datasource[F, G, InterpretedRead[ResourcePath], R, P])
      extends QuasarDatasource[T, F, G, R, P]

  final case class Heavyweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      hw: Datasource[F, G, T[QScriptEducated[T, ?]], R, P])
      extends QuasarDatasource[T, F, G, R, P]

  def lightweight[T[_[_]]] = new PartiallyAppliedLw[T]

  final class PartiallyAppliedLw[T[_[_]]] {
    def apply[F[_], G[_], R, P <: ResourcePathType](
        ds: Datasource[F, G, InterpretedRead[ResourcePath], R, P])
        : QuasarDatasource[T, F, G, R, P] =
      Lightweight(ds)
  }

  def heavyweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      ds: Datasource[F, G, T[QScriptEducated[T, ?]], R, P])
      : QuasarDatasource[T, F, G, R, P] =
    Heavyweight(ds)

  def widenPathType[T[_[_]], F[_], G[_], R, PI <: ResourcePathType, P0 >: PI <: ResourcePathType](
      mds: QuasarDatasource[T, F, G, R, PI])
      : QuasarDatasource[T, F, G, R, P0] = mds match {
    case Lightweight(lw) => Lightweight(Datasource.widenPathType(lw))
    case Heavyweight(hw) => Heavyweight(Datasource.widenPathType(hw))
  }
}
