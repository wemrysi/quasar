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

sealed trait SchedulingError[+I, +II, +C, +CC] extends Product with Serializable

object SchedulerError {
  trait SchedulerError[+I, +C] extends SchedulingError[I, Nothing, C, Nothing]
  trait CreateError[+C] extends SchedulerError[Nothing, C]
  final case class SchedulerUnsupported(kind: SchedulerType, supported: Set[SchedulerType]) extends CreateError[Nothing]
  final case class InitializationError[C](config: C) extends CreateError[C]
  final case class SchedulerNameExists(name: String) extends CreateError[Nothing]

  final case class SchedulerNotFound[I](index: I) extends SchedulerError[I, Nothing]
  final case class DeleteError[C](config: C) extends SchedulerError[Nothing, C]

  trait IntentionError[+I, +C] extends SchedulingError[Nothing, I, Nothing, C]
  final case class IncorrectIntention[C](config: C) extends IntentionError[Nothing, C]
  final case class IntentionNotFound[I](index: I) extends IntentionError[I, Nothing]
}
