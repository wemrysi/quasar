/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError._
import quasar.api.resource.{ResourcePathType, ResourcePath}
import quasar.connector.Datasource
import quasar.contrib.scalaz.{MonadState_, MonadTell_, MonadError_}
import quasar.impl.datasource.EmptyDatasource
import quasar.qscript.InterpretedRead

import cats.effect.Resource

import scalaz.{ISet, Monad, Order, PlusEmpty, Equal, IMap}
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.std.option._

import shims.functorToCats

object MockDatasourceModules {
  trait MockModulesMessage[I] extends Product with Serializable
  final case class Inited[I](i: I) extends MockModulesMessage[I]
  final case class Shutdown[I](i: I) extends MockModulesMessage[I]

  type ModulesMessages[I] = List[MockModulesMessage[I]]
  type MonadModules[F[_], I] = MonadTell_[F, ModulesMessages[I]]

  def apply[
      T[_[_]],
      F[_]: Monad: MonadModules[?[_], I],
      G[_]: PlusEmpty,
      I, C, R](
      supported: ISet[DatasourceType],
      sanitize: C => C,
      emptyResult: R)
      : DatasourceModules[T, F, G, I, C, R, ResourcePathType] = new DatasourceModules[T, F, G, I, C, R, ResourcePathType] {
    def create(i: I, ref: DatasourceRef[C]): Resource[F, ManagedDatasource[T, F, G, R, ResourcePathType]] = {
      val emptyDS: Datasource[F, G, InterpretedRead[ResourcePath], R, ResourcePathType] =
        EmptyDatasource(ref.kind, emptyResult)
      Resource.make(ManagedDatasource.lightweight[T](emptyDS).point[F])(x => ().point[F])
    }
    def sanitizeRef(inp: DatasourceRef[C]): DatasourceRef[C] =
      inp.copy(config = sanitize(inp.config))
    def supportedTypes: F[ISet[DatasourceType]] =
      supported.point[F]
  }

}
