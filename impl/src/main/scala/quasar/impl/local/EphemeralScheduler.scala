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

package quasar.impl.local

import slamdata.Predef._

import quasar.Condition
import quasar.api.schedule.ScheduleError._
import quasar.connector.schedule._
import quasar.impl.storage.IndexedStore

import cats.effect._
import cats.implicits._
import cats.Eq

import argonaut.{Argonaut, Json, EncodeJson, DecodeJson, DecodeResult}, Argonaut._

import fs2.Stream

object EphemeralScheduler {
  trait Period extends Product with Serializable

  object Period {
    final case object Minutely extends Period
    final case object Hourly extends Period
    final case object Daily extends Period

    implicit val encodeJsonPeriod: EncodeJson[Period] = EncodeJson {
      case Minutely => "minutely".asJson
      case Hourly => "hourly".asJson
      case Daily => "daily".asJson
    }
    implicit val decodeJsonPeriod: DecodeJson[Period] = DecodeJson { c =>
      c.as[String] flatMap {
        case "minutely" => DecodeResult.ok(Minutely)
        case "hourly" => DecodeResult.ok(Hourly)
        case "daily" => DecodeResult.ok(Daily)
        case other => DecodeResult.fail(s"$other is incorrect Period", c.history)
      }
    }
    implicit val eqJsonPeriod: Eq[Period] = Eq.fromUniversalEquals
  }

  final case class Task[C](period: Period, content: C)

  object Task {
    implicit def encodeJson[C: EncodeJson]: EncodeJson[Task[C]] = EncodeJson { tsk =>
      Json("period" := tsk.period, "content" := tsk.content)
    }
    implicit def decodeJson[C: DecodeJson]: DecodeJson[Task[C]] = DecodeJson { c =>
      for {
        period <- (c --\ "period").as[Period]
        content <- (c --\ "content").as[C]
      } yield Task(period, content)
    }
  }

  trait Submit[A] {
    def submit[F[_]: ConcurrentEffect](a: A): Stream[F, Unit]
  }

  def apply[F[_]: ContextShift: Effect: Timer, I, C: EncodeJson: DecodeJson](
      freshId: F[I],
      taskStorage: IndexedStore[F, I, Task[C]],
      blocker: Blocker)
      : Schedule[F, Json, I] = new Schedule[F, Json, I] {

    def intentions: Stream[F, (I, Json)] =
      taskStorage.entries map { case (k, tsk) => (k, tsk.asJson) }

    def addIntention(config: Json): F[Either[IncorrectIntention[Json], I]] = config.as[Task[C]].result match {
      case Left(_) => (Left(IncorrectIntention(config)): Either[IncorrectIntention[Json], I]).pure[F]
      case Right(tsk) => for {
        id <- freshId
        _ <- taskStorage.insert(id, tsk)
      } yield (Right(id): Either[IncorrectIntention[Json], I])
    }

    def getIntention(i: I): F[Either[IntentionNotFound[I], Json]] =
      taskStorage.lookup(i) flatMap {
        case None => (Left(IntentionNotFound(i)): Either[IntentionNotFound[I], Json]).pure[F]
        case Some(tsk) => (Right(tsk.asJson): Either[IntentionNotFound[I], Json]).pure[F]
      }

    def editIntention(id: I, config: Json): F[Condition[IntentionError[Json, I]]] = config.as[Task[C]].result match {
      case Left(_) => Condition.abnormal[IntentionError[Json, I]](IncorrectIntention(config)).pure[F]
      case Right(tsk) => taskStorage.insert(id, tsk) as Condition.normal()
    }

    def deleteIntention(i: I): F[Condition[IntentionNotFound[I]]] =
      taskStorage.delete(i) as Condition.normal()

  }

}
