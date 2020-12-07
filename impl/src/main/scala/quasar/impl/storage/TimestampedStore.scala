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

package quasar.impl.storage

import slamdata.Predef._

import quasar.impl.cluster.Timestamped

import cats.Monad
import cats.effect.{Timer, Concurrent, Resource}
import cats.effect.concurrent.Ref
import cats.implicits._

import fs2.Stream
import fs2.concurrent.{NoneTerminatedQueue, Queue}

final class TimestampedStore[F[_]: Monad: Timer, K, V](
    val underlying: IndexedStore[F, K, Timestamped[V]],
    tstamps: Ref[F, Map[K, Long]],
    queue: NoneTerminatedQueue[F, (K, Timestamped[V])])
    extends IndexedStore[F, K, V] {

  def entries: Stream[F, (K, V)] =
    underlying.entries.map(_.traverse(Timestamped.raw(_))).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(Timestamped.raw(_)))

  def insert(k: K, v: V): F[Unit] = for {
    toInsert <- Timestamped.tagged[F, V](v)
    _ <- underlying.insert(k, toInsert)
    _ <- tstamps.update(_ + (k -> toInsert.timestamp))
    _ <- queue.enqueue1(Some((k, toInsert)))
  } yield ()

  def delete(k: K): F[Boolean] = for {
    was <- underlying.lookup(k)
    tmb <- Timestamped.tombstone[F, V]
    res <- underlying.insert(k, tmb)
    _ <- tstamps.update(_ + (k -> tmb.timestamp))
    _ <- queue.enqueue1(Some((k, tmb)))
  } yield was.flatMap(Timestamped.raw(_)).nonEmpty

  def timestamps: F[Map[K, Long]] =
    tstamps.get

  def updates: Stream[F, (K, Timestamped[V])] =
    queue.dequeue
}

object TimestampedStore {
  /** A `TimestampedStore` based on a bounded queue, `insert` and `delete`
    * will block if `updates` is consumed too slowly and the queue fills up.
    */
  def bounded[F[_]: Concurrent: Timer, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]],
      maxSize: Int)
      : Resource[F, TimestampedStore[F, K, V]] =
    Resource.liftF(Queue.boundedNoneTerminated[F, (K, Timestamped[V])](maxSize))
      .flatMap(create(underlying, _))

  /** A `TimestampedStore` based on a circular buffer, `insert` and `delete`
    * will never block, but `updates` may miss events if it is consumed too
    * slowly.
    */
  def circular[F[_]: Concurrent: Timer, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]],
      maxSize: Int)
      : Resource[F, TimestampedStore[F, K, V]] =
    Resource.liftF(Queue.circularBufferNoneTerminated[F, (K, Timestamped[V])](maxSize))
      .flatMap(create(underlying, _))

  ////

  private def create[F[_]: Concurrent: Timer, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]],
      queue: NoneTerminatedQueue[F, (K, Timestamped[V])])
      : Resource[F, TimestampedStore[F, K, V]] = {

    val make: F[TimestampedStore[F, K, V]] =
      for {
        currentStamps <-
          underlying.entries
            .map(_.map(_.timestamp))
            .compile.to(Map)

        tstamps <- Ref[F].of(currentStamps)
      } yield new TimestampedStore(underlying, tstamps, queue)

    // .start to avoid blocking in the bounded case when the queue is full
    Resource.make(make)(_ => Concurrent[F].start(queue.enqueue1(None)).void)
  }
}
