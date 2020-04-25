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

import cats.{Order, Show}
import cats.implicits._

final case class SchedulerType(name: String, version: Long)

object SchedulerType {
  implicit val orderSchedulerType: Order[SchedulerType] = Order.by { t =>
    (t.name, t.version)
  }

  implicit val showSchedulerType: Show[SchedulerType] = Show.show { st =>
    s"SchedulerType(${st.name.show}, ${st.version.show}"
  }
}
