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

package quasar.impl.scheduler

import slamdata.Predef._

import quasar.{Condition, ConditionMatchers, EffectfulQSpec}
import quasar.api.scheduler._, SchedulerError._
import quasar.connector._
import quasar.contrib.scalaz.MonadError_
import quasar.impl.ResourceManager
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}
import quasar.impl.local.{LocalSchedulerModule, LocalScheduler}

import argonaut.{Json, EncodeJson, DecodeJson, Argonaut}, Argonaut._
import argonaut.JsonCats._

import cats.Show
import cats.effect._
import cats.implicits._

import fs2.Stream

import scala.concurrent.ExecutionContext

import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

import DefaultSchedulersSpec._

class DefaultSchedulersSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] with ConditionMatchers {
  val blocker: Blocker = quasar.concurrent.Blocker.cached("default-schedulers-spec")

  implicit val timer = IO.timer(ec)

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))
      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  def module(kind: SchedulerType, err: Option[InitializationError[Json]] = None) = new SchedulerModule[IO, Int] {
    def schedulerType = kind
    def sanitizeConfig(inp: Json) = Json.jString("sanitized")
    def scheduler(config: Json): Resource[IO, Either[InitializationError[Json], Scheduler[IO, Int, Json]]] =
      err match {
        case Some(a) => a.asLeft[Scheduler[IO, Int, Json]].pure[Resource[IO, ?]]
        case None =>
          val scheduler = new Scheduler[IO, Int, Json]  {
            def intentions: Stream[IO, (Int, Json)] =
              Stream.empty
            def addIntention(c: Json): IO[Either[IncorrectIntention[Json], Int]] =
              IO(0.asRight[IncorrectIntention[Json]])
            def getIntention(i: Int): IO[Either[IntentionNotFound[Int], Json]] =
              IO(Json.jNull.asRight[IntentionNotFound[Int]])
            def editIntention(i: Int, config: Json): IO[Condition[IntentionError[Int, Json]]] =
              IO(Condition.normal[IntentionError[Int, Json]]())
            def deleteIntention(i: Int): IO[Condition[IntentionNotFound[Int]]] =
              IO(Condition.normal[IntentionNotFound[Int]]())
          }
          scheduler.asRight[InitializationError[Json]].pure[Resource[IO, ?]]
      }
  }

  def mkSchedulers = {
    val freshId = IO(UUID.randomUUID.toString)
    val fRefs: IO[IndexedStore[IO, String, SchedulerRef[Json]]] =
      IO(new ConcurrentHashMap[String, SchedulerRef[Json]]()) map { (mp: ConcurrentHashMap[String, SchedulerRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, SchedulerRef[Json]](mp, blocker)
      }
    val rCache = ResourceManager[IO, String, Scheduler[IO, UUID, Json]]

    val modules = SchedulerModules[IO, UUID](List(LocalSchedulerModule.ephemeral[IO, Task]))

    for {
      refs <- Resource.liftF(fRefs)
      cache <- rCache
      result <- Resource.liftF(DefaultSchedulers(freshId, refs, cache, modules))
    } yield (refs, result, cache)
  }

  def emptySchedulers = mkSchedulers map (_._2)

  val testRef =
    SchedulerRef(LocalSchedulerModule.ephemeral[IO, Task].schedulerType, "ephemeral", Json.jNull)

  val sanitize: SchedulerRef[Json] => SchedulerRef[Json] = (x: SchedulerRef[Json]) => x.copy(config = Json.jString("sanitized"))

  "schedulers" >> {
    "add scheduler" >> {
      "create and saves scheduler" >>* {
        for {
          (ss, finalize) <- emptySchedulers.allocated
          result <- ss.addScheduler(testRef)
          i = result.toOption.get
          foundRef <- ss.schedulerRef(i)
          _ <- finalize
        } yield {
          result must beRight
          foundRef must beRight
        }
      }
      "rejects duplicate names" >>* {
        for {
          (ss, finalize) <- emptySchedulers.allocated
          original <- ss.addScheduler(testRef)
          duplicate <- ss.addScheduler(testRef)
          _ <- finalize
        } yield {
          original must beRight
          duplicate must beLeft(SchedulerNameExists("ephemeral"))
        }
      }
      "rejects unknown scheduler" >>* {
        val unknownType = SchedulerType("unknown", 0L)
        val unknownRef = SchedulerRef(unknownType, "unknown", Json.jNull)
        for {
          (ss, finalize) <- emptySchedulers.allocated
          result <- ss.addScheduler(unknownRef)
          _ <- finalize
        } yield {
          result must beLeft(SchedulerUnsupported(unknownType, Set(LocalSchedulerModule.ephemeral[IO, Task].schedulerType)))
        }
      }
    }
    "replace destination" >> {
      "replaces a destination" >>* {
        val newRef = testRef.copy(name = "new-name")
        for {
          ((store, ss, _), finalize) <- mkSchedulers.allocated
          _ <- store.insert("1", testRef)
          beforeReplace <- ss.schedulerRef("1")
          replaceResult <- ss.replaceScheduler("1", newRef)
          afterReplace <- ss.schedulerRef("1")
          _ <- finalize
        } yield {
          beforeReplace must beRight(sanitize(testRef))
          replaceResult must beNormal
          afterReplace must beRight(sanitize(newRef))
        }
      }
      "verifies name uniqueness on replacement" >>* {
        val testRef2 =
          testRef.copy(name = "foo-2")
        val testRef3 =
          testRef.copy(name = "foo-2")
        for {
          (ss, finalize) <- emptySchedulers.allocated
          status1 <- ss.addScheduler(testRef)
          status2 <- ss.addScheduler(testRef2)
          id = status1.toOption.get
          replaceStatus <- ss.replaceScheduler(id, testRef2)
          _ <- finalize
        } yield {
          status1 must beRight
          status2 must beRight
          replaceStatus must beAbnormal(SchedulerNameExists(testRef2.name))
        }
      }
      "allows replacement with the same name" >>* {
        val testRef2 = testRef.copy(name = "modified")
        for {
          (ss, finalize) <- emptySchedulers.allocated
          status <- ss.addScheduler(testRef)
          id = status.toOption.get
          replaceStatus <- ss.replaceScheduler(id, testRef2)
          _ <- finalize
        } yield {
          status must beRight
          replaceStatus must beNormal
        }
      }
    }
  }
  "clustering" >> {
    "new ref appeared" >>* {
      for {
        ((store, ss, cache), finalize) <- mkSchedulers.allocated
        _ <- store.insert("foo", testRef)
        s <- ss.schedulerOf("foo")
        _ <- finalize
      } yield {
        s must beRight
      }
    }
    "new unknown ref appeared" >>* {
      val unknownType = SchedulerType("unknown", 2L)
      val unknownRef = SchedulerRef(unknownType, "unknown", Json.jNull)
      mkSchedulers use { case (store, ss, cache) =>
        for {
          _ <- store.insert("unk", unknownRef)
          s <- ss.schedulerOf("unk")
        } yield {
          s must beLeft(SchedulerUnsupported(unknownType, Set(LocalSchedulerModule.ephemeral[IO, Task].schedulerType)))
        }
      }
    }
    "ref disappered" >>* {
      for {
        ((store, ss, cache), finalize) <- mkSchedulers.allocated
        _ <- store.insert("foo", testRef)
        s0 <- ss.schedulerRef("foo")
        _ <- store.delete("foo")
        s1 <- ss.schedulerRef("foo")
        _ <- finalize
      } yield {
        s0 must beRight
        s1 must beLeft
      }
    }
  }
}

object DefaultSchedulersSpec {
  final case class CreateErrorException(ce: CreateError[Json])
    extends Exception(Show[SchedulerError[Int, Json]].show(ce))
  final case class Task(value: Int)

  object Task {
    implicit val submit: LocalScheduler.Submit[IO, Task] = new LocalScheduler.Submit[IO, Task] {
      def submit(a: Task): IO[Unit] = IO.unit
    }
    implicit val encodeJson: EncodeJson[Task] = EncodeJson { tsk =>
      tsk.value.asJson
    }
    implicit val decodeJson: DecodeJson[Task] = DecodeJson { c =>
      c.as[Int].map(Task(_))
    }
  }
}
