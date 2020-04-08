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

import quasar.api.schedule.ScheduleType
import quasar.api.schedule.ScheduleError.InitializationError
import quasar.{concurrent => qc}
import quasar.connector.schedule._
import quasar.impl.local.EphemeralScheduler._
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

import cats.effect._
import cats.implicits._

import fs2.Stream

import argonaut.{Argonaut, Json, EncodeJson, DecodeJson}, Argonaut._

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

object EphermalSchedulerModule {
  // For tests
  final case class PrintLn(value: String)

  object PrintLn {
    implicit val submit: Submit[PrintLn] = new Submit[PrintLn] {
      def submit[F[_]: ConcurrentEffect](pl: PrintLn): F[Unit] =
        Sync[F].delay(println(pl.value))
    }
    implicit val encodeJson: EncodeJson[PrintLn] = EncodeJson { pl =>
      Json("println" := pl.value)
    }
    implicit val decodeJson: DecodeJson[PrintLn] = DecodeJson { c =>
      (c --\ "println").as[String] map (PrintLn(_))
    }
  }

  def apply[F[_]: ContextShift: ConcurrentEffect: Timer, C: EncodeJson: DecodeJson: Submit]
      : ScheduleModule[F, UUID] = {
    lazy val blocker: Blocker = qc.Blocker.cached("local-scheduler")
    new ScheduleModule[F, UUID] {
      val scheduleType: ScheduleType = ScheduleType("local", 1L)

      def sanitizeConfig(config: Json) = config

      def schedule(config: Json): Resource[F, Either[InitializationError[Json], Schedule[F, Json, UUID]]] = {
        val fStore: F[IndexedStore[F, UUID, Intention[C]]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Intention[C]]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        val freshId: F[UUID] =
          Sync[F].delay(UUID.randomUUID)

        val fFlags: F[IndexedStore[F, UUID, Long]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Long]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        for {
          store <- Resource.liftF(fStore)
          flags <- Resource.liftF(fFlags)
          scheduler <- EphemeralScheduler[F, UUID, C](freshId, store, flags, blocker)
        } yield Right(scheduler)
      }
    }
  }
}
