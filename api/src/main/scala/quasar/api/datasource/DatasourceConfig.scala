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

package quasar.api.datasource

import monocle.PLens
import monocle.macros.Lenses
import scalaz.{Apply, Cord, Equal, Order, Show, Traverse1}
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class DatasourceConfig[C](kind: DatasourceType, config: C)

object DatasourceConfig extends DatasourceConfigInstances {
  def pConfig[C, D]: PLens[DatasourceConfig[C], DatasourceConfig[D], C, D] =
    PLens[DatasourceConfig[C], DatasourceConfig[D], C, D](
      _.config)(
      d => _.copy(config = d))
}

sealed abstract class DatasourceConfigInstances extends DatasourceConfigInstances0 {
  implicit def order[C: Order]: Order[DatasourceConfig[C]] =
    Order.orderBy(c => (c.kind, c.config))

  implicit def show[C: Show]: Show[DatasourceConfig[C]] =
    Show.show {
      case DatasourceConfig(t, c) =>
        Cord("DatasourceConfig(") ++ t.show ++ Cord(", ") ++ c.show ++ Cord(")")
    }

  implicit val traverse1: Traverse1[DatasourceConfig] =
    new Traverse1[DatasourceConfig] {
      def foldMapRight1[A, B](fa: DatasourceConfig[A])(z: A => B)(f: (A, => B) => B) =
        z(fa.config)

      def traverse1Impl[G[_]: Apply, A, B](fa: DatasourceConfig[A])(f: A => G[B]) =
        DatasourceConfig.pConfig[A, B].modifyF(f)(fa)
    }
}

sealed abstract class DatasourceConfigInstances0 {
  implicit def equal[C: Equal]: Equal[DatasourceConfig[C]] =
    Equal.equalBy(c => (c.kind, c.config))
}
