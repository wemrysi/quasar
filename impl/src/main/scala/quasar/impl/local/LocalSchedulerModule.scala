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

import quasar.api.scheduler.SchedulerType
import quasar.api.scheduler.SchedulerError.InitializationError
import quasar.{concurrent => qc}
import quasar.connector.scheduler._
import quasar.impl.local.LocalScheduler._
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

//import cats.data.EitherT
import cats.effect._
import cats.implicits._

//import fs2.Stream

import argonaut.{Argonaut, Json, EncodeJson, DecodeJson}, Argonaut._

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

object LocalSchedulerModule {
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

  case class Config(path: String)

  object Config {
    implicit val decode: DecodeJson[Config] = DecodeJson { c =>
      (c --\ "path").as[String].map(Config(_))
    }
  }
/*
  // TODO, we potentially can use smth like `{directory: <Path>}` and put two MapDB there
  def apply[F[_]: ContextShift: ConcurentEffect: Timer, C: EncodeJson: DecodeJson: Submit]
      : SchedulerModule[F, UUID] = {
    lazy val blocker: Blocker = qc.Blocker.cached("local-scheduler")
    new SchedulerModule[F, UUID] {
      private val IntentFile: String = "intentions.db"
      private val FlagsFile: String = "flags.db"

      val schedulerType: SchedulerType = SchedulerType("local", 1L)

      def sanitizeConfig(config: Json) = config

      def scheduler(config: Json)
          : Resource[F, Either[InitializationError[Json], Scheduler[F, Json, UUID]]] = {
        val directory: EitherT[F, InitializationError, Path] = for {
          conf <- attemptConfig[F, Config, InitializationError[Json]](
            config,
            "Failed to decode LocalScheduler config: ")(
            (c, d) => InitializationError(config))
          root <- validatePath(c.path, "Invalid path: ") { d =>
            InitializationError(config)
          }
        } yield root
        for {
          root <- EitherT(Resource.liftF(directory.value))
          intentionsFile = root.resolve(IntentFile)
          flagsFile = root.resolve(FlagsFile)
          intentDB <- EitherT.right(mapDB(intentionsFile))
          flagsFile <- EitherT.right(mapDB(flagsFile))
          intentionsStore <- ConcurrentMapIndexedStore.unhooked
        } yield ()
      }

    }
  }
 */
  def ephemeral[F[_]: ContextShift: ConcurrentEffect: Timer, C: EncodeJson: DecodeJson: Submit]
      : SchedulerModule[F, UUID] = {
    lazy val blocker: Blocker = qc.Blocker.cached("ephemeral-scheduler")
    new SchedulerModule[F, UUID] {

      val schedulerType: SchedulerType = SchedulerType("ephemeral", 1L)

      def sanitizeConfig(config: Json) = config

      def scheduler(config: Json): Resource[F, Either[InitializationError[Json], Scheduler[F, Json, UUID]]] = {
        val fStore: F[IndexedStore[F, UUID, Intention[C]]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Intention[C]]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        val freshId: F[UUID] =
          Sync[F].delay(UUID.randomUUID)

        val fFlags: F[IndexedStore[F, UUID, Long]] =
          Sync[F].delay(new ConcurrentHashMap[UUID, Long]()) map (ConcurrentMapIndexedStore.unhooked(_, blocker))

        for {
          store <- Resource.liftF(fStore)
          flags <- Resource.liftF(fFlags)
          scheduler <- LocalScheduler[F, UUID, C](freshId, store, flags, blocker)
        } yield Right(scheduler)
      }
    }
  }
}
