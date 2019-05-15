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

import slamdata.Predef.{Stream => _, _}

import quasar.api.destination.{DestinationRef, Destinations, DestinationType}
import quasar.connector.{Destination, DestinationModule, ResourceError}
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.RefIndexedStore

import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import cats.effect.IO
import cats.effect.concurrent.Ref
import fs2.Stream
import scalaz.IMap
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import shims._

object DefaultDestinationsSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)
  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  def freshId(ref: Ref[IO, Int]): IO[Int] =
    ref.get.map(_ + 1)

  val modules: IMap[DestinationType, DestinationModule] =
    IMap(MockDestinationModule.destinationType -> MockDestinationModule)

  def manager(
    running: IMap[Int, (Destination[IO], IO[Unit])],
    errors: IMap[Int, Exception]): IO[DestinationManager[Int, Json, IO]] =
    (Ref[IO].of(running) |@| Ref[IO].of(errors))(
      DefaultDestinationManager[Int, IO](modules, _, _))

  def mkDestinations(storeRef: Ref[IO, IMap[Int, DestinationRef[Json]]])
      : IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    for {
      initialIdRef <- Ref.of[IO, Int](0)
      fId = freshId(initialIdRef)
      store = RefIndexedStore[Int, DestinationRef[Json]](storeRef)
      mgr <- manager(IMap.empty, IMap.empty)
    } yield DefaultDestinations[IO, Int, Json](fId, store, mgr)

  def emptyDestinations: IO[Destinations[IO, Stream[IO, ?], Int, Json]] =
    Ref.of[IO, IMap[Int, DestinationRef[Json]]](IMap.empty) >>= (mkDestinations(_))
}
