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

import quasar.Condition
import quasar.api.intentions.IntentionError, IntentionError._

import cats.{Applicative, Show, ~>}
import cats.implicits._

import fs2.Stream

trait Scheduler[F[_], I, C] { scheduler =>
  def entries: Stream[F, (I, C)]
  def addIntention(config: C): F[Either[IncorrectIntention[C], I]]
  def lookupIntention(i: I): F[Either[IntentionNotFound[I], C]]
  def editIntention(i: I, config: C): F[Condition[SchedulingError[I, C]]]
  def deleteIntention(i: I): F[Condition[IntentionNotFound[I]]]

  def mapK[G[_]](f: F ~> G): Scheduler[G, I, C] = new Scheduler[G, I, C] {
    def entries: Stream[G, (I, C)] =
      scheduler.entries.translate(f)
    def addIntention(config: C): G[Either[IncorrectIntention[C], I]] =
      f(scheduler.addIntention(config))
    def lookupIntention(i: I): G[Either[IntentionNotFound[I], C]] =
      f(scheduler.lookupIntention(i))
    def editIntention(i: I, config: C): G[Condition[SchedulingError[I, C]]] =
      f(scheduler.editIntention(i, config))
    def deleteIntention(i: I): G[Condition[IntentionNotFound[I]]] =
      f(scheduler.deleteIntention(i))
  }

  // Scheduler[F, UUID, C] ==> Scheduler[F, Array[Byte], C]
  // UUID => Array[Byte] but Array[Byte] => Either[String, UUID]
  def xmapIndex[OI](
      to: I => OI,
      from: OI => Either[String, I])(
      implicit
      a: Applicative[F],
      s: Show[OI]) = new Scheduler[F, OI, C] {
    def entries: Stream[F, (OI, C)] =
      scheduler.entries.map { case (k, v) => (to(k), v) }

    def addIntention(config: C): F[Either[IncorrectIntention[C], OI]] =
      scheduler.addIntention(config).map(_.map(to))

    def lookupIntention(oi: OI): F[Either[IntentionNotFound[OI], C]] = from(oi) match {
      case Left(msg) =>
        IntentionNotFound(oi, s"i.show can't be converted to internal scheduler type")
          .asLeft[C]
          .pure[F]
      case Right(ii) => scheduler.lookupIntention(ii) map {
        case Right(a) => a.asRight[IntentionNotFound[OI]]
        case Left(IntentionNotFound(x, msg)) => IntentionNotFound(to(x), msg).asLeft[C]
      }
    }

    def editIntention(oi: OI, config: C): F[Condition[SchedulingError[OI, C]]] = from(oi) match {
      case Left(msg) =>
        Condition.abnormal[SchedulingError[OI, C]](
          IntentionNotFound(oi, s"i.show can't be converted to internal scheduler type"))
          .pure[F]
      case Right(ii) => scheduler.editIntention(ii, config) map {
        case Condition.Abnormal(IntentionNotFound(x, msg)) =>
          Condition.abnormal[SchedulingError[OI, C]](IntentionNotFound(to(x), msg))
        case Condition.Abnormal(IncorrectIntention(config, reason)) =>
          Condition.abnormal[SchedulingError[OI, C]](IncorrectIntention(config, reason))
        case Condition.Normal() =>
          Condition.normal[SchedulingError[OI, C]]()
      }
    }

    def deleteIntention(oi: OI): F[Condition[IntentionNotFound[OI]]] = from(oi) match {
      case Left(msg) =>
        Condition.abnormal[IntentionNotFound[OI]](
          IntentionNotFound(oi, s"i.show can't be converted to internal scheduler type"))
          .pure[F]
      case Right(ii) => scheduler.deleteIntention(ii) map {
        case Condition.Abnormal(IntentionNotFound(x, msg)) =>
          Condition.abnormal[IntentionNotFound[OI]](
            IntentionNotFound(to(x), msg))
        case Condition.Normal() =>
          Condition.normal[IntentionNotFound[OI]]()
      }
    }
  }

  // Scheduler[F, I, Task] => Scheduler[F, I, Json]
  // Task => Json but Json => Either[String, Task]
  def xmapConfig[OC](
      to: C => OC,
      from: OC => Either[String, C])(
      implicit
      a: Applicative[F],
      s: Show[OC]) = new Scheduler[F, I, OC] {

    def entries: Stream[F, (I, OC)] =
      scheduler.entries.map(_.map(to))

    def addIntention(config: OC): F[Either[IncorrectIntention[OC], I]] = from(config) match {
      case Left(msg) =>
        IncorrectIntention(config, msg).asLeft[I].pure[F]
      case Right(ic) => scheduler.addIntention(ic) map {
        case Left(IncorrectIntention(x, msg)) => Left(IncorrectIntention(to(x), msg))
        case Right(a) => a.asRight[IncorrectIntention[OC]]
      }
    }

    def lookupIntention(i: I): F[Either[IntentionNotFound[I], OC]] =
      scheduler.lookupIntention(i).map(_.map(to))

    def editIntention(i: I, oc: OC): F[Condition[SchedulingError[I, OC]]] = from(oc) match {
      case Left(msg) =>
        Condition.abnormal[SchedulingError[I, OC]](
          IncorrectIntention(oc, msg))
          .pure[F]
      case Right(ic) => scheduler.editIntention(i, ic) map {
        case Condition.Abnormal(IncorrectIntention(config, reason)) =>
          Condition.abnormal[SchedulingError[I, OC]](
            IncorrectIntention(to(config), reason))
        case Condition.Abnormal(IntentionNotFound(i, msg)) =>
          Condition.abnormal[SchedulingError[I, OC]](
            IntentionNotFound(i, msg))
        case Condition.Normal() =>
          Condition.normal[SchedulingError[I, OC]]()
      }
    }

    def deleteIntention(i: I): F[Condition[IntentionNotFound[I]]] =
      scheduler.deleteIntention(i)
  }
}
