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
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

import fs2.Stream
import fs2.concurrent.{Enqueue, Queue}

final class TimestampedStore[F[_]: Monad: Timer, K, V](
    val underlying: IndexedStore[F, K, Timestamped[V]],
    val updates: Stream[F, (K, Timestamped[V])],
    tstamps: Ref[F, Map[K, Long]],
    enqueue: Enqueue[F, (K, Timestamped[V])])
    extends IndexedStore[F, K, V] {

  def entries: Stream[F, (K, V)] =
    underlying.entries.map(_.traverse(Timestamped.raw(_))).unNone

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(k).map(_.flatMap(Timestamped.raw(_)))

  def insert(k: K, v: V): F[Unit] = for {
    toInsert <- Timestamped.tagged[F, V](v)
    _ <- underlying.insert(k, toInsert)
    _ <- tstamps.update(_ + (k -> toInsert.timestamp))
    _ <- enqueue.enqueue1((k, toInsert))
  } yield ()

  def delete(k: K): F[Boolean] = for {
    was <- underlying.lookup(k)
    tmb <- Timestamped.tombstone[F, V]
    res <- underlying.insert(k, tmb)
    _ <- tstamps.update(_ + (k -> tmb.timestamp))
    _ <- enqueue.enqueue1((k, tmb))
  } yield was.flatMap(Timestamped.raw(_)).nonEmpty

  def timestamps: F[Map[K, Long]] =
    tstamps.get
}

object TimestampedStore {
  /** A `TimestampedStore` based on a bounded queue, `insert` and `delete`
    * will block if `updates` is consumed too slowly and the queue fills up.
    */
  def bounded[F[_]: Concurrent: Timer, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]],
      maxSize: Int)
      : Resource[F, TimestampedStore[F, K, V]] =
    Resource.suspend(Queue.boundedNoneTerminated[F, (K, Timestamped[V])](maxSize) map { q =>
      val enq = new Enqueue[F, (K, Timestamped[V])] {
        def enqueue1(a: (K, Timestamped[V])) = q.enqueue1(Some(a))
        def offer1(a: (K, Timestamped[V])) = q.offer1(Some(a))
      }

      val alloc = for {
        currentStamps <- initialStamps(underlying)
        tstamps <- Ref[F].of(currentStamps)
      } yield new TimestampedStore(underlying, q.dequeue, tstamps, enq)

      Resource.make(alloc)(_ => Concurrent[F].start(q.enqueue1(None)).void)
    })

  /** A `TimestampedStore` that never emits any updates. */
  def silent[F[_]: Concurrent: Timer, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]])
      : Resource[F, TimestampedStore[F, K, V]] =
    Resource.suspend(Deferred[F, Unit] map { ipt =>
      val enq = new Enqueue[F, (K, Timestamped[V])] {
        def enqueue1(a: (K, Timestamped[V])) = Concurrent[F].unit
        def offer1(a: (K, Timestamped[V])) = Concurrent[F].pure(true)
      }

      val alloc = for {
        currentStamps <- initialStamps(underlying)
        tstamps <- Ref[F].of(currentStamps)
        updates = Stream.never[F].interruptWhen(ipt.get.attempt)
      } yield new TimestampedStore(underlying, updates, tstamps, enq)

      Resource.make(alloc)(_ => ipt.complete(()))
    })

  ////

  private def initialStamps[F[_]: Sync, K, V](
      underlying: IndexedStore[F, K, Timestamped[V]])
      : F[Map[K, Long]] =
    underlying.entries
      .map(_.map(_.timestamp))
      .compile.to(Map)
}
