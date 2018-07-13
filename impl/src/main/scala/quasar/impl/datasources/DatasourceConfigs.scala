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

import slamdata.Predef.{Boolean, Option, Unit}
import quasar.api.ResourceName
import quasar.api.datasource.DatasourceType
import quasar.higher.HFunctor

import scalaz.{~>, Functor, IMap, InvariantFunctor}
import scalaz.syntax.functor._

/** A primitive facility for managing datasource configurations.
  *
  * All mutating operations overwrite existing configurations as this is intended
  * to be used as a dependency in another context, such as `DefaultDatasources`,
  * that decides whether doing so is appropriate.
  */
trait DatasourceConfigs[F[_], C] {
  /** Associate the given configuration with `name`, replacing any
    * existing association.
    */
  def add(name: ResourceName, config: DatasourceConfig[C]): F[Unit]

  /** The set of configured datasources. */
  def configured: F[IMap[ResourceName, DatasourceType]]

  /** Returns the configuration associated with `name` or `None` if none exists. */
  def lookup(name: ResourceName): F[Option[DatasourceConfig[C]]]

  /** Removes any association with the given name, returning whether one existed. */
  def remove(name: ResourceName): F[Boolean]

  /** Renames the association of `src` to `dst`, replacing any existing
    * association at `dst`.
    */
  def rename(src: ResourceName, dst: ResourceName): F[Unit]
}

object DatasourceConfigs extends DatasourceConfigsInstances

sealed abstract class DatasourceConfigsInstances {
  implicit def hFunctor[C]: HFunctor[DatasourceConfigs[?[_], C]] =
    new HFunctor[DatasourceConfigs[?[_], C]] {
      def hmap[F[_], G[_]](dsc: DatasourceConfigs[F, C])(f: F ~> G): DatasourceConfigs[G, C] =
        new DatasourceConfigs[G, C] {
          def add(name: ResourceName, config: DatasourceConfig[C]): G[Unit] =
            f(dsc.add(name, config))

          def configured: G[IMap[ResourceName, DatasourceType]] =
            f(dsc.configured)

          def lookup(name: ResourceName): G[Option[DatasourceConfig[C]]] =
            f(dsc.lookup(name))

          def remove(name: ResourceName): G[Boolean] =
            f(dsc.remove(name))

          def rename(src: ResourceName, dst: ResourceName): G[Unit] =
            f(dsc.rename(src, dst))
        }
    }

  implicit def invariantFunctor[F[_]: Functor]: InvariantFunctor[DatasourceConfigs[F, ?]] =
    new InvariantFunctor[DatasourceConfigs[F, ?]] {
      def xmap[A, B](fa: DatasourceConfigs[F, A], f: A => B, g: B => A): DatasourceConfigs[F, B] =
        new DatasourceConfigs[F, B] {
          def add(name: ResourceName, config: DatasourceConfig[B]): F[Unit] =
            fa.add(name, config.map(g))

          def configured: F[IMap[ResourceName, DatasourceType]] =
            fa.configured

          def lookup(name: ResourceName): F[Option[DatasourceConfig[B]]] =
            fa.lookup(name).map(_.map(_.map(f)))

          def remove(name: ResourceName): F[Boolean] =
            fa.remove(name)

          def rename(src: ResourceName, dst: ResourceName): F[Unit] =
            fa.rename(src, dst)
        }
    }
}
