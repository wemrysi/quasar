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

import quasar.EffectfulQSpec

import quasar.Condition
import quasar.api.scheduler._, SchedulerError._
import quasar.connector._
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}

import argonaut.Json
import argonaut.JsonCats._

import cats.Show
import cats.effect._
import cats.implicits._

import fs2.Stream

import scala.concurrent.ExecutionContext

import SchedulerModulesSpec._

final class SchedulerModulesSpec(implicit ec: ExecutionContext) extends EffectfulQSpec[IO] {
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

  def mkModules(lst: List[SchedulerModule[IO, Int]]) = SchedulerModules[IO, Int](lst)

  val fooKind = SchedulerType("foo", 1L)
  val barKind = SchedulerType("bar", 2L)

  "supported types" >> {
    "empty" >>* {
      for {
        actual <- mkModules(List()).supportedTypes
      } yield {
        actual must_=== Set()
      }
    }
    "non-empty" >>* {
      val expected = Set(fooKind)
      for {
        actual <- mkModules(List(module(fooKind))).supportedTypes
      } yield {
        actual must_=== expected
      }
    }
  }

  "sanitizeRef" >> {
    val modules = mkModules(List(module(fooKind)))
    val supported = SchedulerRef(fooKind, "supported", Json.jString("foo"))
    val unsupported = SchedulerRef(barKind, "unsupported", Json.jString("bar"))

    modules.sanitizeRef(supported) must_=== supported.copy(config = Json.jString("sanitized"))
    modules.sanitizeRef(unsupported) must_=== unsupported.copy(config = Json.jEmptyObject)
  }

  "create" >> {
    "work for supported" >>* {
      val ref = SchedulerRef(fooKind, "supported", Json.jString(""))
      mkModules(List(module(fooKind))).create(ref).value use { res =>
        IO(res must beRight)
      }
    }
    "doesn't work for unsupported" >>* {
      val ref = SchedulerRef(barKind, "unsupported", Json.jString(""))
      mkModules(List(module(fooKind))).create(ref).value use { res =>
        IO(res must beLeft(SchedulerUnsupported(barKind, Set(fooKind))))
      }
    }
  }

  "errors with initialization error" >>* {
    val malformed = SchedulerType("malformed", 1L)
    val invalid = SchedulerType("invalid", 1L)
    val connFailed = SchedulerType("conn-failed", 1L)
    val accessDenied = SchedulerType("access-denied", 1L)

    val malformedRef = SchedulerRef(malformed, "doesn't matter", Json.jString("malformed"))
    val connFailedRef = SchedulerRef(connFailed, "doesn't matter", Json.jString("connection failed"))
    val invalidRef = SchedulerRef(invalid, "doesn't matter", Json.jString("invalid"))
    val accessDeniedRef = SchedulerRef(accessDenied, "doesn't matter", Json.jString("access denied"))

    val malformedErr = MalformedConfiguration(malformed, Json.jString("a"), "malformed")
    val invalidErr = InvalidConfiguration(invalid, Json.jString("b"), Set("invalid"))
    val connErr = ConnectionFailed(connFailed, Json.jString("c"), new Exception("conn"))
    val accessErr = AccessDenied(accessDenied, Json.jString("d"), "access denied")

    val modules = mkModules(List(
      module(malformed, Some(malformedErr)),
      module(invalid, Some(invalidErr)),
      module(connFailed, Some(connErr)),
      module(accessDenied, Some(accessErr))))

    for {
      (malformedS, _) <- modules.create(malformedRef).value.allocated
      (invalidS, _) <- modules.create(invalidRef).value.allocated
      (connFailedS, _) <- modules.create(connFailedRef).value.allocated
      (accessDeniedS, _) <- modules.create(accessDeniedRef).value.allocated
    } yield {
      malformedS must beLeft(malformedErr)
      invalidS must beLeft(invalidErr)
      connFailedS must beLeft(connErr)
      accessDeniedS must beLeft(accessErr)
    }
  }
}

object SchedulerModulesSpec {
  final case class CreateErrorException(ce: CreateError[Json])
    extends Exception(Show[SchedulerError[Int, Json]].show(ce))
}
