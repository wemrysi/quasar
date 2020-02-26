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

import quasar.api.resource.ResourcePath

import java.time.Instant

import cats.{Eq, Show}
import cats.implicits._

import monocle.Prism

import shims.{equalToCats, showToCats}

sealed trait PushMeta[O] extends Product with Serializable {
  def path: ResourcePath
  def latestStatus: Status
}

object PushMeta {
  final case class Full[O](
      path: ResourcePath,
      latestStatus: Status)
      extends PushMeta[O]

  final case class Incremental[O](
      path: ResourcePath,
      latestStatus: Status,
      createdAt: Instant,
      resumeConfig: ResumeConfig[O],
      initialOffset: Option[OffsetKey.Actual[O]])
      extends PushMeta[O]

  def full[O]: Prism[PushMeta[O], (ResourcePath, Status)] =
    Prism.partial[PushMeta[O], (ResourcePath, Status)] {
      case Full(p, s) => (p, s)
    } ((Full[O] _).tupled)

  def incremental[O]: Prism[PushMeta[O], (ResourcePath, Status, Instant, ResumeConfig[O], Option[OffsetKey.Actual[O]])] =
    Prism.partial[PushMeta[O], (ResourcePath, Status, Instant, ResumeConfig[O], Option[OffsetKey.Actual[O]])] {
      case Incremental(p, s, t, r, o) => (p, s, t, r, o)
    } ((Incremental[O] _).tupled)

  implicit def pushMetaEq[O]: Eq[PushMeta[O]] = {
    implicit val instantEq = Eq.fromUniversalEquals[Instant]
    Eq.by(m => (full[O].getOption(m), incremental[O].getOption(m)))
  }

  implicit def pushMetaShow[O]: Show[PushMeta[O]] =
    Show show {
      case Full(p, s) =>
        s"Full(${p.show}, ${s.show})"

      case Incremental(p, s, i, c, o) =>
        s"Incremental(${p.show}, ${s.show}, $i, ${c.show}, ${o.show})"
    }
}
