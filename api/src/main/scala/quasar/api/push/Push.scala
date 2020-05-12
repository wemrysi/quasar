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

package quasar.api.push

import slamdata.Predef._

import java.time.Instant

import cats.{Eq, Show}
import cats.implicits._

import monocle.macros.Lenses

import shims.{equalToCats, showToCats}

@Lenses
final case class Push[O, Q](
    config: PushConfig[O, Q],
    createdAt: Instant,
    status: Status)

object Push {
  implicit def pushEq[O, Q: Eq]: Eq[Push[O, Q]] = {
    implicit val instantEq = Eq.fromUniversalEquals[Instant]
    Eq.by(p => (p.config, p.createdAt, p.status))
  }

  implicit def pushShow[O, Q: Show]: Show[Push[O, Q]] =
    Show.show(p =>
      s"Push(${p.config.show}, ${p.createdAt}, ${p.status.show})")
}
