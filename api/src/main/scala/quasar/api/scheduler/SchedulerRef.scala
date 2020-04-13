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

package quasar.api.scheduler

import slamdata.Predef._

import cats.{Eq, Order, Show}
import cats.implicits._

import monocle.PLens

final case class SchedulerRef[C](kind: SchedulerType, name: String, config: C)

object SchedulerRef {
  def atMostRenamed[C: Eq](a: SchedulerRef[C], b: SchedulerRef[C]) =
    a.kind === b.kind && a.config === b.config

  def pConfig[C, D]: PLens[SchedulerRef[C], SchedulerRef[D], C, D] =
    PLens[SchedulerRef[C], SchedulerRef[D], C, D](
      _.config)(
      d => _.copy(config = d))

  implicit def orderSchedulerRef[C: Order]: Order[SchedulerRef[C]] = Order.by { ref =>
    (ref.kind, ref.name, ref.config)
  }

  implicit def eqSchedulerRef[C: Eq]: Eq[SchedulerRef[C]] = Eq.by { ref =>
    (ref.kind, ref.name, ref.config)
  }

  implicit def showSchedulerRef[C: Show]: Show[SchedulerRef[C]] = Show.show { ref =>
    s"SchedulerRef(${ref.kind.show}, ${ref.name.show}, ${ref.config.show}"
  }
}
