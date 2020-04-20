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

sealed trait IntentionError[+I, +C] extends Product with Serializable

object IntentionError {
  final case class IncorrectIntention[C](config: C) extends IntentionError[Nothing, C]
  final case class IntentionNotFound[I](index: I) extends IntentionError[I, Nothing]

  implicit def show[I: Show, C: Show]: Show[IntentionError[I, C]] = Show.show {
    case IncorrectIntention(config) => s"IncorrectIntention(${config.show})"
    case IntentionNotFound(index) => s"IntentionNotFound(${index.show})"
  }
}
