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

package quasar.api.destination

import slamdata.Predef._

import monocle.macros.Lenses
import scalaz.std.anyVal._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.{Equal, Order, Show}

@Lenses
final case class DestinationType(name: String, version: Int)

object DestinationType extends DestinationTypeInstances

sealed abstract class DestinationTypeInstances {
  implicit val equal: Equal[DestinationType] =
    Equal.equalBy(t => (t.name, t.version))

  implicit val order: Order[DestinationType] =
    Order.orderBy(t => (t.name, t.version))

  implicit val show: Show[DestinationType] =
    Show.shows {
      case DestinationType(n, v) =>
        "DestinationType(" + n.shows + ", " + v.shows + ")"
    }
}
