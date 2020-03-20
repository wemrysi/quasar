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

package quasar.api.schedule

import slamdata.Predef._

import cats.{Eq, Order, Show}
import cats.implicits._

import monocle.PLens

final case class ScheduleRef[C](kind: ScheduleType, name: String, config: C)

object ScheduleRef {
  def atMostRenamed[C: Eq](a: ScheduleRef[C], b: ScheduleRef[C]) =
    a.kind === b.kind && a.config === b.config

  def pConfig[C, D]: PLens[ScheduleRef[C], ScheduleRef[D], C, D] =
    PLens[ScheduleRef[C], ScheduleRef[D], C, D](
      _.config)(
      d => _.copy(config = d))

  implicit def orderScheduleRef[C: Order]: Order[ScheduleRef[C]] = Order.by { ref =>
    (ref.kind, ref.name, ref.config)
  }
  implicit def showScheduleRef[C: Show]: Show[ScheduleRef[C]] = Show.show { ref =>
    s"ScheduleRef(${ref.kind.show}, ${ref.name.show}, ${ref.config.show}"
  }
}
