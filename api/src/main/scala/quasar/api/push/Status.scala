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

import monocle.Prism

sealed trait Status extends Product with Serializable {
  def startedAt: Instant
  def limit: Option[Long]
}

object Status {
  final case class Finished(startedAt: Instant, finishedAt: Instant, limit: Option[Long])
      extends Status
  final case class Running(startedAt: Instant, limit: Option[Long])
      extends Status
  final case class Canceled(startedAt: Instant, canceledAt: Instant, limit: Option[Long])
      extends Status
  final case class Failed(startedAt: Instant, failedAt: Instant, limit: Option[Long], cause: Throwable)
      extends Status

  val finished: Prism[Status, (Instant, Instant, Option[Long])] =
    Prism.partial[Status, (Instant, Instant, Option[Long])] {
      case Finished(st, fn, lm) => (st, fn, lm)
    } (Finished.tupled)

  val running: Prism[Status, (Instant, Option[Long])] =
    Prism.partial[Status, (Instant, Option[Long])] {
      case Running(st, lm) => (st, lm)
    } (Running.tupled)

  val canceled: Prism[Status, (Instant, Instant, Option[Long])] =
    Prism.partial[Status, (Instant, Instant, Option[Long])] {
      case Canceled(st, fn, lm) => (st, fn, lm)
    } (Canceled.tupled)

  val failed: Prism[Status, (Instant, Instant, Option[Long], Throwable)] =
    Prism.partial[Status, (Instant, Instant, Option[Long], Throwable)] {
      case Failed(st, fn, lm, th) => (st, fn, lm, th)
    } (Failed.tupled)

  implicit val statusEq: Eq[Status] =
    Eq.fromUniversalEquals

  implicit val statusShow: Show[Status] = Show show {
    case Finished(startedAt, finishedAt, limit) =>
      s"Finished($startedAt, $finishedAt, $limit)"
    case Running(startedAt, limit) =>
      s"Running($startedAt, $limit)"
    case Canceled(startedAt, canceledAt, limit) =>
      s"Canceled($startedAt, $canceledAt, $limit)"
    case Failed(startedAt, finishedAt, limit, cause) =>
      s"Failed($startedAt, $finishedAt, $limit)\n\n$cause"
  }
}
