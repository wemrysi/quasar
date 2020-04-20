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

package quasar.api.intentions

import slamdata.Predef._

import cats.Show
import cats.implicits._

sealed trait IntentionError[+I, +II, +C] extends Product with Serializable

object IntentionError {
  final case class SchedulerNotFound[I](index: I) extends IntentionError[I, Nothing, Nothing]

  sealed trait SchedulingError[+I, +C] extends IntentionError[Nothing, I, C]
  final case class IncorrectIntention[C](config: C, reason: String) extends SchedulingError[Nothing, C]
  final case class IntentionNotFound[I](index: I, reason: String) extends SchedulingError[I, Nothing]

  object SchedulingError {
    implicit def show[I: Show, C: Show]: Show[SchedulingError[I, C]] = Show.show {
      case IncorrectIntention(config, reason) => s"IncorrectIntention(${config.show}, ${reason.show})"
      case IntentionNotFound(index, reason) => s"IntentionNotFound(${index.show}, ${reason.show})"
    }
  }

  implicit def show[I: Show, II: Show, C: Show]: Show[IntentionError[I, II, C]] = Show.show {
    case s: SchedulingError[II, C] => s.show
    case SchedulerNotFound(index) => s"SchedulerNotFound(${index.show})"
  }
}
