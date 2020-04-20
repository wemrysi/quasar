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

package quasar.connector.scheduler

import slamdata.Predef._

import quasar.api.scheduler._, SchedulerError._

import argonaut.Json
import cats.effect._

import scala.util.Either

trait SchedulerModule {
  def schedulerType: SchedulerType
  def sanitizeConfig(config: Json): Json
  def scheduler[F[_]: ContextShift: ConcurrentEffect: Timer](config: Json)
      : Resource[F, Either[InitializationError[Json], Scheduler[F, Array[Byte], Json]]]
}
