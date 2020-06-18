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

import cats.{Hash, Order, Show}

import scala.{Product, Serializable}

sealed trait Reconfiguration extends Product with Serializable

object Reconfiguration {

  implicit val orderHash: Order[Reconfiguration] with Hash[Reconfiguration] =
    new Order[Reconfiguration] with Hash[Reconfiguration] {

      def hash(r: Reconfiguration) = r match {
        case Reset => 0
        case Preserve => 1
      }

      def compare(r1: Reconfiguration, r2: Reconfiguration) =
        hash(r1) - hash(r2)
    }

  implicit val show: Show[Reconfiguration] =
    Show.fromToString[Reconfiguration]

  case object Reset extends Reconfiguration
  case object Preserve extends Reconfiguration
}
