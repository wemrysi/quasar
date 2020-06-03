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
import quasar.api.intentions.IntentionError, IntentionError._
import quasar.connector._
import quasar.connector.scheduler._
import quasar.contrib.scalaz.MonadError_
import quasar.impl.ResourceManager
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

import argonaut.Json
import argonaut.JsonCats._

import cats.effect._
import cats.implicits._

import fs2.Stream

import scala.concurrent.ExecutionContext

import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

class DefaultSchedulersSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] with ConditionMatchers {
  val blocker: Blocker = quasar.concurrent.Blocker.cached("default-schedulers-spec")

  implicit val timer = IO.timer(ec)

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def builder(kind: SchedulerType, err: Map[Json, InitializationError[Json]])
      : SchedulerBuilder[IO] =
    new SchedulerBuilder[IO] {
      def schedulerType = kind
      def sanitizeConfig(inp: Json) = Json.jString("sanitized")
      def scheduler(config: Json)
          : Resource[IO, Either[InitializationError[Json], Scheduler[IO, Array[Byte], Json]]] =
        err.get(config) match {
          case Some(e) =>
            e.asLeft[Scheduler[IO, Array[Byte], Json]].pure[Resource[IO, ?]]
          case None =>
            val scheduler = new Scheduler[IO, Array[Byte], Json]  {
              def entries: Stream[IO, (Array[Byte], Json)] =
                Stream.empty
              def addIntention(c: Json): IO[Either[IncorrectIntention[Json], Array[Byte]]] =
                IO(UUID.randomUUID.toString.getBytes.asRight[IncorrectIntention[Json]])
              def lookupIntention(i: Array[Byte]): IO[Either[IntentionNotFound[Array[Byte]], Json]] =
                IO(Json.jNull.asRight[IntentionNotFound[Array[Byte]]])
              def editIntention(i: Array[Byte], config: Json): IO[Condition[SchedulingError[Array[Byte], Json]]] =
                IO(Condition.normal[SchedulingError[Array[Byte], Json]]())
              def deleteIntention(i: Array[Byte]): IO[Condition[IntentionNotFound[Array[Byte]]]] =
                IO(Condition.normal[IntentionNotFound[Array[Byte]]]())
            }
            scheduler.asRight[InitializationError[Json]].pure[Resource[IO, ?]]
        }
    }

  def mkSchedulers(errors: Map[Json, InitializationError[Json]] = Map.empty) = {
    val freshId = IO(UUID.randomUUID.toString)
    val fRefs: IO[IndexedStore[IO, String, SchedulerRef[Json]]] =
      IO(new ConcurrentHashMap[String, SchedulerRef[Json]]()) map { (mp: ConcurrentHashMap[String, SchedulerRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, SchedulerRef[Json]](mp, blocker)
      }
    val rCache = ResourceManager[IO, String, Scheduler[IO, Array[Byte], Json]]

    for {
      refs <- Resource.liftF(fRefs)
      cache <- rCache
      builders <- Resource.liftF(SchedulerBuilders[IO](List(builder(testType, errors))))
      result <- Resource.liftF(DefaultSchedulers(freshId, refs, cache, builders))
    } yield (refs, result, cache)
  }

  def emptySchedulers = mkSchedulers() map (_._2)

  val testType =
    SchedulerType("test", 1L)

  val testRef =
    SchedulerRef(testType, "test", Json.jNull)

  val sanitize: SchedulerRef[Json] => SchedulerRef[Json] = (x: SchedulerRef[Json]) => x.copy(config = Json.jString("sanitized"))

  "schedulers" >> {
    "enable/disableModule" >>* {
      val unknownType = SchedulerType("unknown", 0L)
      val unknownRef = SchedulerRef(unknownType, "unknown", Json.jNull)
      for {
        (ss, finalize) <- emptySchedulers.allocated
        result0 <- ss.addScheduler(unknownRef)
        _ <- ss.enableModule(builder(unknownType, Map.empty))
        result1 <- ss.addScheduler(unknownRef)
        id = result1.toOption.get
        _ <- ss.removeScheduler(id)
        _ <- ss.disableModule(unknownType)
        result2 <- ss.addScheduler(unknownRef)
        _ <- finalize
      } yield {
        result0 must beLeft(SchedulerUnsupported(unknownType, Set(testType)))
        result1 must beRight
        result2 must beLeft(SchedulerUnsupported(unknownType, Set(testType)))
      }
    }
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
          duplicate must beLeft(SchedulerNameExists("test"))
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
          result must beLeft(SchedulerUnsupported(unknownType, Set(testType)))
        }
      }
    }
    "replace scheduler" >> {
      "replaces a scheduler" >>* {
        val newRef = testRef.copy(name = "new-name")
        for {
          ((store, ss, _), finalize) <- mkSchedulers().allocated
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

      "doesn't replace when config invalid" >>* {
        val err3: InitializationError[Json] =
          MalformedConfiguration(testType, Json.jString("three"), "3 isn't a config!")

        val invalidRef =
          testRef.copy(config = Json.jString("invalid"))

        for {
          ((store, ss, _), finalize) <- mkSchedulers(Map(Json.jString("invalid") -> err3)).allocated
          c1 <- ss.addScheduler(testRef)
          i = c1.toOption.get
          r1 <- store.lookup(i)
          c2 <- ss.replaceScheduler(i, invalidRef)
          r2 <- store.lookup(i)
          _ <- finalize
        } yield {
          r1 must beSome(testRef)
          c2 must beAbnormal(err3)
          r2 must beSome(testRef)
        }
      }
    }
  }
  "clustering" >> {
    "new ref appeared" >>* {
      for {
        ((store, ss, cache), finalize) <- mkSchedulers().allocated
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
      mkSchedulers() use { case (store, ss, cache) =>
        for {
          _ <- store.insert("unk", unknownRef)
          s <- ss.schedulerOf("unk")
        } yield {
          s must beLeft(SchedulerUnsupported(unknownType, Set(testType)))
        }
      }
    }
    "ref disappered" >>* {
      for {
        ((store, ss, cache), finalize) <- mkSchedulers().allocated
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
