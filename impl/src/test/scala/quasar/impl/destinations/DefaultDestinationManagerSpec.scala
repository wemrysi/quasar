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

import quasar.api.destination.DestinationError.InitializationError
import quasar.api.destination.DestinationType
import quasar.api.resource.ResourcePath
import quasar.connector.{Destination, DestinationModule, MonadResourceErr, ResourceError, ResultSink, ResultType, TableColumn}
import quasar.contrib.scalaz.MonadError_

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.{ConcurrentEffect, ContextShift, IO, Resource, Timer}
import eu.timepit.refined.auto._
import fs2.Stream
import scalaz.{\/, Applicative, IMap, NonEmptyList}
import scalaz.syntax.applicative._
import scalaz.syntax.either._
import shims._

object DefaultDestinationManagerSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  val NullDestinantionType = DestinationType("null", 1L, 1L)

  class NullCsvSink[F[_]: Applicative] extends ResultSink[F] {
    val resultType = ResultType.Csv()
    def apply(dst: ResourcePath, result: (List[TableColumn], Stream[F, Byte])): F[Unit] =
      ().point[F]
  }

  class NullDestination[F[_]: Applicative] extends Destination[F] {
    def destinationKind: DestinationType = NullDestinantionType
    def sinks = NonEmptyList(new NullCsvSink[F]())
  }

  object NullDestinationModule extends DestinationModule {
    def destinationType = NullDestinantionType
    def sanitizeDestinationConfig(config: Json) = config

    def destination[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](config: Json)
        : F[InitializationError[Json] \/ Resource[F, Destination[F]]] =
      (new NullDestination[F]: Destination[F])
        .point[Resource[F, ?]].right[InitializationError[Json]].point[F]
  }

  val modules: IMap[DestinationType, DestinationModule] =
    IMap(NullDestinantionType -> NullDestinationModule)

  "manage destinations" >> {
    "initializes a destination" >> {
      1 must_=== 1
    }
  }
}
