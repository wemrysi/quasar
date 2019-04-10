/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.api.datasource

import scalaz.{Equal, Show}

sealed abstract class Capability

object Capability {
  case object ReadOnly extends Capability
  case object WriteOnly extends Capability
  case object ReadWrite extends Capability

  implicit val show: Show[Capability] =
    Show.shows {
      case ReadOnly => "Capability(ReadOnly)"
      case WriteOnly => "Capability(WriteOnly)"
      case ReadWrite => "Capability(ReadWrite)"
    }

  implicit val equal: Equal[Capability] = Equal.equalA
}
