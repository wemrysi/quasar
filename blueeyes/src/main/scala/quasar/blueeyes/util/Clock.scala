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

package quasar.blueeyes.util

import quasar.blueeyes._

import java.time.{Instant, LocalDateTime, Period}

import cats.effect.IO
import scalaz.Monad
import scalaz.syntax.monad._

trait Clock[F[_]] {

  /** Returns the current time.
    */
  def now: F[LocalDateTime]

  def instant: F[Instant]

  def nanoTime: F[Long]

  /** Times how long the specified future takes to be delivered.
    */
  def time[T](f: () => F[T])(implicit F: Monad[F]): F[(Period, T)] = for {
    start <- now
    t <- f()
    end <- now
  } yield (start until end, t)

  /** Times a pure function.
    */
  def timePure[T](f: () => T)(implicit F: Monad[F]): F[(Period, T)] =
    time[T](() => f().pure[F])
}

object Clock {
  val System = ClockSystem.realtimeClock
}

trait ClockSystem {
  implicit val realtimeClock: Clock[IO] = new Clock[IO] {
    def now: IO[LocalDateTime] = IO(dateTime.now())
    def instant: IO[Instant] = IO(quasar.blueeyes.instant.now())
    def nanoTime: IO[Long] = IO(System.nanoTime())
  }
}

object ClockSystem extends ClockSystem
