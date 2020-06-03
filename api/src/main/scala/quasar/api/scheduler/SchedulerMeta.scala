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

import quasar.Condition
import slamdata.Predef._

import cats.Show
import cats.implicits._

import shims.{showToCats, showToScalaz}

final case class SchedulerMeta(
    kind: SchedulerType,
    name: String,
    status: Condition[Exception])

object SchedulerMeta {
  def fromOption(
      kind: SchedulerType,
      name: String,
      optErr: Option[Exception]) =
    SchedulerMeta(kind, name, Condition.optionIso.reverseGet(optErr))

  implicit val show: Show[SchedulerMeta] = Show.show { m =>
    implicit val exShow: Show[Exception] =
      Show.show(_.getMessage)

    s"SchedulerMeta(${m.kind.show}, ${m.name.show}, ${m.status.show})"
  }

}
