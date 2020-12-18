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

import java.time.{Duration, Instant}

import cats.{Eq, Show}

sealed trait Status extends Product with Serializable {
  def startedAt: Instant
  def limit: Option[Long]
}

object Status {
  final case class Unknown(startedAt: Instant, limit: Option[Long])
      extends Status

  sealed trait Active extends Status

  final case class Accepted(startedAt: Instant, limit: Option[Long])
      extends Active
  final case class Running(startedAt: Instant, limit: Option[Long])
      extends Active
  final case class Cancelling(startedAt: Instant, limit: Option[Long], runningAt: Option[Instant])
      extends Active

  sealed trait Terminal extends Status

  final case class Finished(startedAt: Instant, finishedAt: Instant, limit: Option[Long])
      extends Terminal
  final case class Canceled(startedAt: Instant, canceledAt: Instant, limit: Option[Long])
      extends Terminal
  final case class Failed(startedAt: Instant, failedAt: Instant, limit: Option[Long], reason: String)
      extends Terminal

  val elapsed: Terminal => Duration = {
    case Finished(s, e, _) => Duration.between(s, e)
    case Canceled(s, e, _) => Duration.between(s, e)
    case Failed(s, e, _, _) => Duration.between(s, e)
  }

  implicit def statusEq[S <: Status]: Eq[S] =
    Eq.fromUniversalEquals

  implicit def statusShow[S <: Status]: Show[S] =
    Show show {
      case Unknown(startedAt, limit) =>
        s"Unknown($startedAt, $limit)"
      case Accepted(startedAt, limit) =>
        s"Accepted($startedAt, $limit)"
      case Running(startedAt, limit) =>
        s"Running($startedAt, $limit)"
      case Cancelling(startedAt, limit, runningAt) =>
        s"Cancelling($startedAt, $limit, $runningAt)"
      case Finished(startedAt, finishedAt, limit) =>
        s"Finished($startedAt, $finishedAt, $limit)"
      case Canceled(startedAt, canceledAt, limit) =>
        s"Canceled($startedAt, $canceledAt, $limit)"
      case Failed(startedAt, finishedAt, limit, reason) =>
        s"Failed($startedAt, $finishedAt, $limit, $reason)"
    }
}
