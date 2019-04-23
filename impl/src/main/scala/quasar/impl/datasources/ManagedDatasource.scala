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

package quasar.impl.datasources

import slamdata.Predef._
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.Datasource
import quasar.qscript.{InterpretedRead, QScriptEducated}

import scalaz.~>

/** An active datasource with a lifecycle under the control of a `DatasourceManager`. */
sealed trait ManagedDatasource[T[_[_]], F[_], G[_], R] {
  import ManagedDatasource._

  def kind: DatasourceType =
    this match {
      case ManagedLightweight(lw) => lw.kind
      case ManagedHeavyweight(hw) => hw.kind
    }

  def pathIsResource(path: ResourcePath): F[Boolean] =
    this match {
      case ManagedLightweight(lw) => lw.pathIsResource(path)
      case ManagedHeavyweight(hw) => hw.pathIsResource(path)
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def prefixedChildPaths(prefixPath: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]] =
    this match {
      case ManagedLightweight(lw) => lw.prefixedChildPaths(prefixPath).asInstanceOf[F[Option[G[(ResourceName, ResourcePathType)]]]]
      case ManagedHeavyweight(hw) => hw.prefixedChildPaths(prefixPath).asInstanceOf[F[Option[G[(ResourceName, ResourcePathType)]]]]
    }

  def modify[V[_], W[_], S](
      f: Datasource.Aux[F, G, ?, R, ResourcePathType] ~> Datasource.Aux[V, W, ?, S, ResourcePathType])
      : ManagedDatasource[T, V, W, S] =
    this match {
      case ManagedLightweight(lw) => ManagedLightweight(f(lw))
      case ManagedHeavyweight(hw) => ManagedHeavyweight(f(hw))
    }
}

object ManagedDatasource {
  final case class ManagedLightweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      lw: Datasource.Aux[F, G, InterpretedRead[ResourcePath], R, P])
      extends ManagedDatasource[T, F, G, R]

  final case class ManagedHeavyweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      hw: Datasource.Aux[F, G, T[QScriptEducated[T, ?]], R, P])
      extends ManagedDatasource[T, F, G, R]

  def lightweight[T[_[_]]] = new PartiallyAppliedLw[T]
  final class PartiallyAppliedLw[T[_[_]]] {
    def apply[F[_], G[_], R, P <: ResourcePathType](
        ds: Datasource.Aux[F, G, InterpretedRead[ResourcePath], R, P])
        : ManagedDatasource[T, F, G, R] =
      ManagedLightweight(ds)
  }

  def heavyweight[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      ds: Datasource.Aux[F, G, T[QScriptEducated[T, ?]], R, P])
      : ManagedDatasource[T, F, G, R] =
    ManagedHeavyweight(ds)
}
