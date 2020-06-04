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

package quasar.connector

import quasar.api.push.{OffsetKey, OffsetPath}

import scala.StringContext

import cats.Show
import cats.syntax.show._

import skolems.∃

final case class Offset(path: OffsetPath, value: ∃[OffsetKey.Actual])

object Offset {
  implicit val offsetShow: Show[Offset] =
    Show show { o =>
      val ps = o.path.map(_.fold(f => s".${f}", i => s"[$i]"))
      s"Offset(${ps.toList.mkString}, ${o.value.value.show})"
    }
}
