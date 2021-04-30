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

package quasar

import slamdata.Predef._

import quasar.contrib.cats.hash.toHashing
import quasar.contrib.cats.eqv.toEquiv

import java.util.concurrent.TimeUnit

import scala.collection.concurrent.TrieMap
import scala.concurrent.duration._

import cats.effect.{Concurrent, Timer}
import cats.effect.concurrent.{Deferred, Ref}
import cats.kernel.Hash
import cats.implicits._

final class RateLimiter[F[_]: Concurrent: Timer, A: Hash] private () {

  // TODO make this clustering-aware
  private val configs: TrieMap[A, RateLimiterConfig] =
    new TrieMap[A, RateLimiterConfig](toHashing[A], toEquiv[A])

  // TODO make this clustering-aware
  private val states: TrieMap[A, Ref[F, State[F]]] =
    new TrieMap[A, Ref[F, State[F]]](toHashing[A], toEquiv[A])

  def apply(key: A, max: Int, window: FiniteDuration)
      : F[RateLimiterEffects[F]] =
    for {
      config <- Concurrent[F] delay {
        val c = RateLimiterConfig(max, window)
        configs.putIfAbsent(key, c).getOrElse(c)
      }

      now <- nowF
      maybeR <- Ref.of[F, State[F]](State[F](Processing(now, 0), List()))
      stateRef <- Concurrent[F] delay {
        states.putIfAbsent(key, maybeR).getOrElse(maybeR)
      }
    } yield {
      RateLimiterEffects[F](
        limit(config, stateRef),
        backoff(config, stateRef))
    }

  /* The backoff function is as a damage control measure, providing a way for
   * information to be communicated back to the rate limiter.
   *
   * It waits a full window before trying again.
   *
   * It initiates draining upon waking from sleep so that the queued resquests will
   * be processed.
   */
  private def backoff(config: RateLimiterConfig, stateRef: Ref[F, State[F]])
      : F[Unit] = {
    val window = config.window

    val back = for {
      now <- nowF
      modified <- stateRef modify[F[Unit]] {
        case State(s, queue) =>
          //println(s">>>>1 state when backoff: $s sleeping ${window}")
          val state = State(Waiting(now + window), queue)
          val effect = Timer[F].sleep(window) >> drain(config, stateRef)
          (state, effect)
      }
    } yield modified

    back.flatten
  }

  /* Recursively drains the queue of deferred requests until it is empty.
   *
   * If we are currently in the waiting state, we assume we've already slept long
   * enough and thus transition into the processing state.
   *
   * If we are currently in the processing state, we check which window we're in
   * and if we're within the request limit.
   */
  private def drain(config: RateLimiterConfig, stateRef: Ref[F, State[F]]): F[Unit] = {
    val window = config.window
    val max = config.max

    val back = for {
      now <- nowF
      modified <- stateRef modify[F[Unit]] {
        // nothing available to drain, reset to processing and stop the recursion
        case State(Waiting(next), Nil) =>
          //println(s">>>>2 drain nil")
          (State(Processing(next, 0), List()), ().pure[F])

        // nothing available to drain, stop the recusion
        case State(s @ Processing(_, _), Nil) =>
          //println(s">>>>3 drain nil 2")
          (State(s, List()), ().pure[F])

        // we know we've already slept if we reach this point
        case State(Waiting(next), queue) =>
          //println(s">>>>4 drain wait next $next")
          val state = State(Processing(next, 1), queue.dropRight(1))
          val effect = queue.last.complete(()) >> drain(config, stateRef)
          (state, effect)

        // something available to drain
        case State(Processing(current, count), queue) =>
          if (current + window <= now) { // outside current window, reset the state and loop
            //println(s">>>>5 drain next ${current + window}")
            val state = State(Processing(current + window, 0), queue)
            val effect = drain(config, stateRef)
            (state, effect)
          } else if (count < max) { // in current window, and within the limit
            //println(s">>>>6 drain within count $count with queue size ${queue.length}")
            val state = State(Processing(current, count + 1), queue.dropRight(1))
            val effect = queue.last.complete(()) >> drain(config, stateRef)
            (state, effect)
          } else { // in current window, limit exceeded
            //println(s">>>>7 drain exceeded next ${current + window} sleeping ${(current + window) - now}")
            val state = State(Waiting(current + window), queue)
            val effect = Timer[F].sleep((current + window) - now) >> drain(config, stateRef)
            (state, effect)
          }
      }
    } yield modified

    back.flatten
  }

  private def limit(config: RateLimiterConfig, stateRef: Ref[F, State[F]]): F[Unit] = {
    val window = config.window
    val max = config.max

    val back = for {
      now <- nowF
      modified <- stateRef modify[F[Unit]] {
        // always enqueue when waiting
        // we don't need to check the time because we know we'll drain when it's time
        case State(wait @ Waiting(_), queue) =>
          //println(s">>>>8 waiting")
          val deferred = Deferred.unsafe[F, Unit]
          val state = State[F](wait, deferred :: queue)
          (state, deferred.get)

        // the queue is empty, so attempt to continue
        case State(Processing(current, count), Nil) =>
          if (current + window <= now) { // past current window, reset the state and loop
            //println(s">>>>9 processing nil next")
            val state = State[F](Processing(current + window, 0), Nil)
            (state, limit(config, stateRef))
          } else if (count < max) { // in the current window and within the limit
            //println(s">>>>10 processing nil within with current $current and now $now and count $count")
            val state = State[F](Processing(current, count + 1), Nil)
            (state, ().pure[F])
          } else { // in current window and limit is exceeded
            //println(s">>>>11 processing nil wait with current $current and now $now and count $count sleeping ${(current + window) - now}")
            val deferred = Deferred.unsafe[F, Unit]
            val state = State[F](Waiting(current + window), List(deferred))
            val sleep = Timer[F].sleep((current + window) - now)
            // start draining so we can get the deferred
            val started = Concurrent[F].start(sleep >> drain(config, stateRef))
            val effect = started >> deferred.get
            (state, effect)
          }

        // when the queue is non-empty, we enqueue all new requests
        case State(p @ Processing(current, count), queue) =>
          //println(s">>>>12 processing queue")
          val deferred = Deferred.unsafe[F, Unit]
          val state = State(p, deferred :: queue)
          (state, deferred.get)

      }
    } yield modified

    back.flatten
  }

  private val nowF: F[FiniteDuration] =
    Timer[F].clock.realTime(TimeUnit.MILLISECONDS).map(_.millis)

  private case class State[F[_]](status: Status, queue: List[Deferred[F, Unit]])

  private sealed trait Status
  private case class Waiting(nextWindow: FiniteDuration) extends Status
  private case class Processing(currentWindow: FiniteDuration, count: Int) extends Status
}

object RateLimiter {
  def apply[F[_]: Concurrent: Timer, A: Hash](freshKey: F[A])
      : F[RateLimiting[F, A]] =
    Concurrent[F].delay(RateLimiting[F, A](
      new RateLimiter[F, A](), freshKey))
}
