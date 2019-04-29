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

import quasar.contrib.refined._
import quasar.fp.numeric.Positive

import eu.timepit.refined.scalaz._
import monocle.macros.Lenses
import scalaz.{Cord, Order, Show}
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._

@Lenses
final case class DestinationType(name: DestinationType.Name, version: Positive, minSupportedVersion: Positive)

object DestinationType extends DestinationTypeInstances {
  type Name = datasource.Name
  type NameP = datasource.NameP
}

sealed abstract class DestinationTypeInstances {
  implicit val order: Order[DestinationType] =
    Order.orderBy(t => (t.name, t.version, t.minSupportedVersion))

  implicit val show: Show[DestinationType] =
    Show.show {
      case DestinationType(n, v, minV) =>
        Cord("DestinationType(") ++ n.show ++ Cord(", ") ++ v.show ++ Cord(", ") ++ minV.show ++ Cord(")")
    }
}
