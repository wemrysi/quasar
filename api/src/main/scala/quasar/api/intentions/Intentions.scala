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

import quasar.Condition
import quasar.api.intentions.IntentionError._

trait Intentions[F[_], G[_], I, II, C] {
  def allIntentions: G[(I, II, C)]
  def schedulerIntentions(schedulerId: I): F[Either[SchedulerNotFound[I], G[(II, C)]]]
  def add(schedulerId: I, config: C): F[Either[IntentionError[I, II, C], II]]
  def lookup(schedulerId: I, intentionId: II): F[Either[IntentionError[I, II, C], C]]
  def edit(schedulerId: I, intentionId: II, config: C): F[Condition[IntentionError[I, II, C]]]
  def delete(schedulerId: I, intentionId: II): F[Condition[IntentionError[I, II, C]]]
}
