/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar

import slamdata.Predef._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import org.specs2.mutable.Specification

import fs2.Stream

import cats.effect.{IO, Sync, Timer}
import cats.effect.laws.util.TestContext
import cats.kernel.Hash
import cats.implicits._

object RateLimiterSpec extends Specification {

  implicit def executionContext: ExecutionContext = ExecutionContext.Implicits.global
  implicit val timer: Timer[IO] = IO.timer(executionContext)

  "rate limiter" should {
    "output events with real time" >> {
      "one event in one window" in {
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 1, 1.seconds).unsafeRunSync()

        val back = Stream.eval_(effect) ++ Stream.emit(1)

        back.compile.toList.unsafeRunSync() mustEqual(List(1))
      }

      "two events in one window" in {
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 2, 1.seconds).unsafeRunSync()

        val back =
          Stream.eval_(effect) ++ Stream.emit(1) ++
            Stream.eval_(effect) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "two events in two windows" in {
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 1, 1.seconds).unsafeRunSync()

        val back =
          Stream.eval_(effect) ++ Stream.emit(1) ++
            Stream.eval_(effect) ++ Stream.emit(2)

        back.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
      }

      "events from two tokens" in {
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater).unsafeRunSync()

        val effect1: IO[Unit] = rl(1, 1, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(2, 1, 1.seconds).unsafeRunSync()

        val back1 =
          Stream.eval_(effect1) ++ Stream.emit(1) ++
            Stream.eval_(effect1) ++ Stream.emit(2)

        val back2 =
          Stream.eval_(effect2) ++ Stream.emit(3) ++
            Stream.eval_(effect2) ++ Stream.emit(4)

        back1.compile.toList.unsafeRunSync() mustEqual(List(1, 2))
        back2.compile.toList.unsafeRunSync() mustEqual(List(3, 4))
      }
    }

    "output events with simulated time" >> {
      "one event per second" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 1, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "one event per two seconds" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 1, 2.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "two events per second" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 2, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "three events per second" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "with a caution of 0.75" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](0.75, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect: IO[Unit] = rl(1, 4, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(6)

        ctx.tick(1.seconds)
        a mustEqual(8)
      }

      "do not overwrite configs (use existing config)" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect1: IO[Unit] = rl(1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(1, 3, 1.seconds).unsafeRunSync()

        var a: Int = 0

        val run =
          effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1) >>
            effect2 >> IO.delay(a += 1)

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)

        ctx.tick(1.seconds)
        a mustEqual(6)
      }

      "support two tokens on the same schedule" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect1: IO[Unit] = rl(1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(2, 3, 1.seconds).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1)

        val run2 =
          effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(3)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(6)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(8)
      }

      "support two tokens on different schedules" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val effect1: IO[Unit] = rl(1, 2, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(2, 2, 2.seconds).unsafeRunSync()

        var a1: Int = 0
        var a2: Int = 0

        val run1 =
          effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1) >>
            effect1 >> IO.delay(a1 += 1)

        val run2 =
          effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1) >>
            effect2 >> IO.delay(a2 += 1)

        run1.unsafeRunAsyncAndForget()
        run2.unsafeRunAsyncAndForget()

        a1 mustEqual(2)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(4)
        a2 mustEqual(2)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(4)

        ctx.tick(1.seconds)
        a1 mustEqual(6)
        a2 mustEqual(6)
      }
    }

    "handle reset request" >> {
      "reset for unknown key has no effect" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val reset = rl.reset(key)
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          (reset >> effectF).flatMap(effect =>
            IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }

      "reset for known but unused key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val reset = rl.reset(key)
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectF.flatMap(effect =>
            reset >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }

      "reset state for known key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val reset = rl.reset(key)
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectF.flatMap(effect =>
            effect >> IO.delay(a += 1) >>
            reset >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)
      }
    }

    "handle plus one request" >> {
      "modify state for unknown key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val plusOne = rl.plusOne(key)
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          (plusOne >> effectF).flatMap(effect =>
            IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)
      }

      "modify state for known key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val plusOne = rl.plusOne(key)
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectF.flatMap(effect =>
            plusOne >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)
      }
    }

    "handle configure request" >> {
      "add config for unknown key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val configure = rl.configure(key, RateLimiterConfig(2, 1.seconds))
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          (configure >> effectF).flatMap(effect =>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }

      "ignore config added for known key" in {
        val ctx = TestContext()
        val rl = RateLimiter[IO, Int](1.0, NoopRateLimitUpdater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key: Int = 17

        val configure = rl.configure(key, RateLimiterConfig(2, 1.seconds))
        val effectF = rl(key, 1, 1.seconds)

        var a: Int = 0

        val run =
          effectF.flatMap(effect =>
            configure >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1) >>
            effect >> IO.delay(a += 1))

        run.unsafeRunAsyncAndForget()

        a mustEqual(1)

        ctx.tick(1.seconds)
        a mustEqual(2)

        ctx.tick(1.seconds)
        a mustEqual(3)

        ctx.tick(1.seconds)
        a mustEqual(4)
      }
    }

    "send updates through the updater" >> {
      "one event per second" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater
        val rl = RateLimiter[IO, Int](1.0, updater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key = 17

        val effect: IO[Unit] = rl(key, 1, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(17))

        (effect >> effect >> effect).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(17))
        updater.resets must containTheSameElementsAs(List())

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 17))
        updater.resets must containTheSameElementsAs(List(17))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 17, 17))
        updater.resets must containTheSameElementsAs(List(17, 17))
      }

      "two events per second" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater
        val rl = RateLimiter[IO, Int](1.0, updater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key = 17

        val effect: IO[Unit] = rl(key, 2, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(17))

        (effect >> effect >> effect >> effect).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(17, 17))
        updater.resets must containTheSameElementsAs(List())

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 17, 17, 17))
        updater.resets must containTheSameElementsAs(List(17))
      }

      "two keys" in {
        val ctx = TestContext()
        val updater = new TestRateLimitUpdater
        val rl = RateLimiter[IO, Int](1.0, updater)(Sync[IO], ctx.timer[IO], Hash[Int]).unsafeRunSync()

        val key1 = 17
        val key2 = 18

        val effect1: IO[Unit] = rl(key1, 1, 1.seconds).unsafeRunSync()
        val effect2: IO[Unit] = rl(key2, 2, 1.seconds).unsafeRunSync()

        updater.configs must containTheSameElementsAs(List(17, 18))

        (effect1 >> effect1 >> effect1 >> effect1).unsafeRunAsyncAndForget()
        (effect2 >> effect2 >> effect2 >> effect2).unsafeRunAsyncAndForget()

        updater.plusOnes must containTheSameElementsAs(List(17, 18, 18))
        updater.resets must containTheSameElementsAs(List())

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 18, 18, 17, 18, 18))
        updater.resets must containTheSameElementsAs(List(17, 18))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 18, 18, 17, 18, 18, 17))
        updater.resets must containTheSameElementsAs(List(17, 18, 17))

        ctx.tick(1.seconds)
        updater.plusOnes must containTheSameElementsAs(List(17, 18, 18, 17, 18, 18, 17, 17))
        updater.resets must containTheSameElementsAs(List(17, 18, 17, 17))
      }
    }
  }
}
