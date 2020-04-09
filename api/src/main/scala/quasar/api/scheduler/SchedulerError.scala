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

sealed trait SchedulerError[+C, +CC, +I, +II] extends Product with Serializable

object SchedulerError {
  trait SchedulerrError[+C, +I] extends SchedulerError[C, Nothing, I, Nothing]
  final case class CreateError[C](config: C) extends SchedulerrError[C, Nothing]
  final case class DeleteError[C](config: C) extends SchedulerrError[C, Nothing]
  final case class NotFoundError[I](index: I) extends SchedulerrError[Nothing, I]
  final case class InitializationError[C](config: C) extends SchedulerrError[C, Nothing]

  trait IntentionError[+C, +I] extends SchedulerError[Nothing, C, Nothing, I]
  final case class IncorrectIntention[C](config: C) extends IntentionError[C, Nothing]
  final case class IntentionNotFound[I](index: I) extends IntentionError[Nothing, I]
}
