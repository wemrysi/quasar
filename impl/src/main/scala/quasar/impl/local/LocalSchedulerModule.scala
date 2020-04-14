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

import quasar.api.scheduler._, SchedulerError._
import quasar.{concurrent => qc}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.local.LocalScheduler._
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}
import quasar.impl.store.{Store, StoreError}

import cats.data.EitherT
import cats.Show
import cats.effect._
import cats.implicits._

import argonaut.{Argonaut, Json, EncodeJson, DecodeJson, CodecJson}, Argonaut._

import java.nio.file.Path
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

object LocalSchedulerModule {
  case class Config(path: String)

  object Config {
    implicit val decode: DecodeJson[Config] = DecodeJson { c =>
      (c --\ "path").as[String].map(Config(_))
    }
  }

  def freshUUID[F[_]: Sync]: F[UUID] = Sync[F].delay(UUID.randomUUID)

  def apply[
      F[_]: ContextShift: ConcurrentEffect: Timer: MonadError_[?[_], StoreError],
      C: EncodeJson: DecodeJson: Submit[F, ?]: Show]
      : SchedulerModule[F, UUID] = {
    lazy val blocker: Blocker = qc.Blocker.cached("local-scheduler")
    new SchedulerModule[F, UUID] {
      private val IntentFile: String = "intentions.db"
      private val FlagsFile: String = "flags.db"

      val schedulerType: SchedulerType = SchedulerType("local", 1L)

      def sanitizeConfig(config: Json) = config

      def scheduler(config: Json)
          : Resource[F, Either[InitializationError[Json], Scheduler[F, UUID, Json]]] = {
        val directory: EitherT[F, InitializationError[Json], Path] = for {
          conf <- attemptConfig[F, Config, InitializationError[Json]](
            config,
            "Failed to decode LocalScheduler config: ")(
            (c, d) => MalformedConfiguration(schedulerType, c, d): InitializationError[Json])
          root <- validatedPath(conf.path, "Invalid path: ") { d =>
            InvalidConfiguration(schedulerType, config, Set(d)): InitializationError[Json]
          }
        } yield root
        val eitRes: EitherT[Resource[F, ?], InitializationError[Json], Scheduler[F, UUID, Json]] = for {
          root <- EitherT(Resource.liftF(directory.value))
          intentionsFile = root.resolve(IntentFile)
          flagsFile = root.resolve(FlagsFile)
          scheduler <- EitherT.right(for {
            intentStore <- Store.codecStore(intentionsFile, CodecJson.derived[Intention[C]], blocker)
            flagsStore <- Store.codecStore(flagsFile, CodecJson.derived[Long], blocker)
            scheduler <- LocalScheduler(freshUUID, intentStore, flagsStore, blocker)
          } yield scheduler)
        } yield scheduler
        eitRes.value
      }
    }
  }

  def ephemeral[F[_]: ContextShift: ConcurrentEffect: Timer, C: EncodeJson: DecodeJson: Submit[F, ?]]
      : SchedulerModule[F, UUID] = {
    lazy val blocker: Blocker = qc.Blocker.cached("ephemeral-scheduler")
    new SchedulerModule[F, UUID] {

      val schedulerType: SchedulerType = SchedulerType("ephemeral", 1L)

      def sanitizeConfig(config: Json) = Json.jString("sanitized")

      def scheduler(config: Json): Resource[F, Either[InitializationError[Json], Scheduler[F, UUID, Json]]] = {
        val fStore: F[IndexedStore[F, UUID, Intention[C]]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Intention[C]]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        val fFlags: F[IndexedStore[F, UUID, Long]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Long]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        for {
          store <- Resource.liftF(fStore)
          flags <- Resource.liftF(fFlags)
          scheduler <- LocalScheduler[F, UUID, C](freshUUID, store, flags, blocker)
        } yield Right(scheduler)
      }
    }
  }
}
