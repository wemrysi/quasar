/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.impl.destinations

import slamdata.Predef._

import quasar.Condition
import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.{DestinationError, DestinationName, DestinationRef, DestinationType}
import quasar.api.resource.ResourcePath
import quasar.connector.{Destination, DestinationModule, MonadResourceErr, ResourceError, ResultSink, ResultType, TableColumn}
import quasar.contrib.scalaz.MonadError_

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.Stream
import scalaz.{\/, Applicative, IMap, ISet, NonEmptyList}
import scalaz.std.anyVal._
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import shims._

object DefaultDestinationManagerSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val NullDestinationType = DestinationType("null", 1L, 1L)

  class NullCsvSink[F[_]: Applicative] extends ResultSink[F] {
    val resultType = ResultType.Csv()
    def apply(dst: ResourcePath, result: (List[TableColumn], Stream[F, Byte])): F[Unit] =
      ().point[F]
  }

  class NullDestination[F[_]: Applicative] extends Destination[F] {
    def destinationKind: DestinationType = NullDestinationType
    def sinks = NonEmptyList(new NullCsvSink[F]())
  }

  object NullDestinationModule extends DestinationModule {
    def destinationType = NullDestinationType
    def sanitizeDestinationConfig(config: Json) =
      Json.jString("foobar")

    def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](config: Json)
        : F[InitializationError[Json] \/ Resource[F, Destination[F]]] =
      (new NullDestination[F]: Destination[F])
        .point[Resource[F, ?]].right[InitializationError[Json]].point[F]
  }

  val modules: IMap[DestinationType, DestinationModule] =
    IMap(NullDestinationType -> NullDestinationModule)

  val manager: IO[DestinationManager[Int, Json, IO]] = {
    val running = Ref.of[IO, IMap[Int, (Destination[IO], IO[Unit])]](IMap.empty)
    val currentErrors = Ref.of[IO, IMap[Int, Exception]](IMap.empty)

    (running |@| currentErrors)(DefaultDestinationManager[Int, IO](modules, _, _))
  }

  "default destination manager" >> {
    "returns configured destination types" >> {
      manager.flatMap(_.supportedDestinationTypes).unsafeRunSync must_== ISet.singleton(NullDestinationType)
    }

    "initializes a destination" >> {
      val ref = DestinationRef(NullDestinationType, DestinationName("foo_null"), Json.jEmptyString)

      val inited = for {
        mgr <- manager
        _ <- mgr.initDestination(1, ref)
        justSaved <- mgr.destinationOf(1)
      } yield justSaved

      inited.unsafeRunSync.map(_.destinationKind) must beSome(NullDestinationType)
    }

    "rejects a destination of an unknown type" >> {
      val notKnown = DestinationType("notknown", 1L, 1L)
      val ref = DestinationRef(notKnown, DestinationName("notknown"), Json.jEmptyString)

      manager.flatMap(_.initDestination(1, ref)).unsafeRunSync must beLike {
        case Condition.Abnormal(DestinationError.DestinationUnsupported(k, s)) =>
          k must_== notKnown
          s must_== ISet.singleton(NullDestinationType)
      }
    }

    "removes a destination on shutdown" >> {
      val ref = DestinationRef(NullDestinationType, DestinationName("foo_null"), Json.jEmptyString)

      val shutdown = for {
        mgr <- manager
        _ <- mgr.initDestination(1, ref)
        beforeShutdown <- mgr.destinationOf(1)
        _ <- mgr.shutdownDestination(1)
        afterShutdown <- mgr.destinationOf(1)
      } yield (beforeShutdown, afterShutdown)

      val (before, after) = shutdown.unsafeRunSync

      before must beSome
      after must beNone
    }

    "calls module implementation of sanitizedRef" >> {
      val ref = DestinationRef(NullDestinationType, DestinationName("foo_null"), Json.jEmptyString)

      val sanitized = for {
        mgr <- manager
        sanitized = mgr.sanitizedRef(ref)
      } yield sanitized

      sanitized.unsafeRunSync must_== DestinationRef(NullDestinationType, DestinationName("foo_null"), Json.jString("foobar"))
    }
  }
}
