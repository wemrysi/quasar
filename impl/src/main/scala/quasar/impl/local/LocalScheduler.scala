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
import quasar.api.scheduler.SchedulerError._
import quasar.connector.scheduler._
import quasar.impl.storage.IndexedStore

import cats.effect._
import cats.implicits._
import cats.{Eq, Show}

import argonaut.{Argonaut, Json, EncodeJson, DecodeJson, DecodeResult}, Argonaut._

import fs2.Stream

import simulacrum.typeclass

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration._

object LocalScheduler {
  trait Period extends Product with Serializable

  object Period {
    final case object Minutely extends Period
    final case object Hourly extends Period
    final case object Daily extends Period
    final case class OnceAt(timestamp: Long) extends Period

    implicit val encodeJsonPeriod: EncodeJson[Period] = EncodeJson {
      case Minutely => "minutely".asJson
      case Hourly => "hourly".asJson
      case Daily => "daily".asJson
      case OnceAt(value) => value.asJson
    }
    implicit val decodeJsonPeriod: DecodeJson[Period] = DecodeJson { c =>
      val stringRes: DecodeResult[Period] = c.as[String] flatMap {
        case "minutely" => DecodeResult.ok(Minutely)
        case "hourly" => DecodeResult.ok(Hourly)
        case "daily" => DecodeResult.ok(Daily)
        case other => DecodeResult.fail(s"$other is incorrect Period", c.history)
      }
      val longRes: DecodeResult[Period] = c.as[Long] map (OnceAt(_))
      longRes ||| stringRes
    }
    implicit val eqJsonPeriod: Eq[Period] = Eq.fromUniversalEquals
    implicit val showPeriod: Show[Period] = Show.fromToString
  }

  final case class Intention[C](period: Period, content: C)

  object Intention {
    implicit def encodeJson[C: EncodeJson]: EncodeJson[Intention[C]] = EncodeJson { tsk =>
      Json("period" := tsk.period, "content" := tsk.content)
    }
    implicit def decodeJson[C: DecodeJson]: DecodeJson[Intention[C]] = DecodeJson { c =>
      for {
        period <- (c --\ "period").as[Period]
        content <- (c --\ "content").as[C]
      } yield Intention(period, content)
    }
    implicit def show[C: Show]: Show[Intention[C]] = Show.show[Intention[C]] { (i: Intention[C]) =>
      s"Intention(${i.period.show}, ${i.content.show})"
    }
  }

  @typeclass trait Submit[A] {
    def submit[F[_]: ConcurrentEffect](a: A): F[Unit]
  }

  import Period._
  import Submit.ops._

  def apply[F[_]: ContextShift: ConcurrentEffect: Timer, I, C: EncodeJson: DecodeJson: Submit](
      freshId: F[I],
      storage: IndexedStore[F, I, Intention[C]],
      flags: IndexedStore[F, I, Long],
      blocker: Blocker)
      : Resource[F, Scheduler[F, Json, I]] = {

    def runIntention(k: I, intention: Intention[C], dt: ZonedDateTime): F[Unit] = for {
      hour <- Sync[F].delay(dt.getHour())
      minute <- Sync[F].delay(dt.getMinute())
      _ <- intention.period match {
        case Minutely =>
          intention.content.submit
        case Hourly if minute === 0 =>
          intention.content.submit
        case Daily if (minute === 0 && hour === 0) =>
          intention.content.submit
        case OnceAt(millis) =>
          val nowMS = dt.toInstant().toEpochMilli()
          if (nowMS < millis) ().pure[F]
          else flags.lookup(k) flatMap {
            case None => intention.content.submit >> flags.insert(k, nowMS)
            case Some(_) => ().pure[F]
          }
        case _ => ().pure[F]
      }
    } yield ()

    val run: F[Unit] = for {
      now <- Timer[F].clock.realTime(SECONDS)
      zone <- Sync[F].delay(ZoneId.systemDefault())
      dt = Instant.ofEpochSecond(now).atZone(zone)
      _ <- storage.entries.evalMap({
        case (k, intention) => runIntention(k, intention, dt)
      }).compile.drain
    } yield ()

    val scheduler = new Scheduler[F, Json, I] {

      def intentions: Stream[F, (I, Json)] =
        storage.entries map { case (k, tsk) => (k, tsk.asJson) }

      def addIntention(config: Json): F[Either[IncorrectIntention[Json], I]] = config.as[Intention[C]].result match {
        case Left(_) =>
          (Left(IncorrectIntention(config)): Either[IncorrectIntention[Json], I]).pure[F]
        case Right(tsk) => for {
          id <- freshId
          _ <- storage.insert(id, tsk)
        } yield (Right(id): Either[IncorrectIntention[Json], I])
      }

      def getIntention(i: I): F[Either[IntentionNotFound[I], Json]] =
        storage.lookup(i) map {
          case None => Left(IntentionNotFound(i))
          case Some(tsk) => Right(tsk.asJson)
        }

      def editIntention(id: I, config: Json): F[Condition[IntentionError[Json, I]]] = config.as[Intention[C]].result match {
        case Left(_) =>
          Condition.abnormal[IntentionError[Json, I]](IncorrectIntention(config)).pure[F]
        case Right(tsk) =>
          storage.lookup(id) flatMap {
            case None =>
              Condition.abnormal[IntentionError[Json, I]](IntentionNotFound(id)).pure[F]
            case Some(_) =>
              storage.insert(id, tsk) as Condition.normal()
          }
      }

      def deleteIntention(i: I): F[Condition[IntentionNotFound[I]]] =
        storage.delete(i) map { (existed: Boolean) =>
          if (existed) Condition.normal() else Condition.abnormal[IntentionNotFound[I]](IntentionNotFound(i))
        }
    }

    Stream.emit(scheduler)
      .concurrently((Stream.eval(run) ++ Stream.sleep(1.minute)).repeat)
      .compile
      .resource
      .lastOrError
  }
}
