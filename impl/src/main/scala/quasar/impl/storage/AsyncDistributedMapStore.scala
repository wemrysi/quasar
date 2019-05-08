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

package quasar.impl.storage

import slamdata.Predef._

import cats.effect._
import cats.effect.concurrent.Deferred
import cats.syntax.functor._
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._

import fs2.concurrent.Queue
import fs2.Stream

import io.atomix.core.map.{AsyncDistributedMap, MapEvent, MapEventListener}, MapEvent.Type._
import io.atomix.core.iterator.AsyncIterator

import java.util.concurrent.CompletableFuture

final class AsyncDistributedMapStore[F[_]: Async: ContextShift, K, V](
    mp: AsyncDistributedMap[K, V])
    extends IndexedStore[F, K, V] {

  import AsyncDistributedMapStore._

  def entries: Stream[F, (K, V)] = for {
    iterator <- Stream.eval(Sync[F].delay(mp.entrySet.iterator))
    entry <- fromIterator[F, java.util.Map.Entry[K, V]](iterator)
  } yield (entry.getKey, entry.getValue)

  def lookup(k: K): F[Option[V]] =
    toF(mp get k) map (Option(_))

  def insert(k: K, v: V): F[Unit] =
    toF(mp.put(k, v)) as (())

  def delete(k: K): F[Boolean] =
    toF(mp.remove(k)) map { (x: V) => !Option(x).isEmpty }
}

object AsyncDistributedMapStore {
  trait IndexedStoreEvent[K, +V]

  final case class Insert[K, V](k: K, v: V) extends IndexedStoreEvent[K, V]
  final case class Remove[K](k: K) extends IndexedStoreEvent[K, Nothing]

  def eventStream[F[_]: ConcurrentEffect: ContextShift, K, V](mp: AsyncDistributedMap[K, V]): Stream[F, IndexedStoreEvent[K, V]] = {
    val F = ConcurrentEffect[F]
    def run(f: F[Unit]): Unit = F.runAsync(f)(_ => IO.unit).unsafeRunSync
    def listener(cb: IndexedStoreEvent[K, V] => Unit): MapEventListener[K, V] = { event => event.`type` match {
      case INSERT => cb(Insert(event.key, event.newValue))
      case UPDATE => cb(Insert(event.key, event.newValue))
      case REMOVE => cb(Remove(event.key))
    }}
    for {
      q <- Stream.eval(Queue.bounded[F, IndexedStoreEvent[K, V]](1))
      handler = listener(x => run(q.enqueue1(x)))
      _ <- Stream.eval(toF(mp addListener handler))
      res <- q.dequeue.onFinalize(toF(mp removeListener handler) as (()))
    } yield res
  }

  def fromIterator[F[_]: ContextShift: Async, A](iterator: AsyncIterator[A]): Stream[F, A] = {
    def getNext(i: AsyncIterator[A]): F[Option[(A, AsyncIterator[A])]] = for {
      hasNext <- toF(i.hasNext())
      step <- if (hasNext.booleanValue) toF(i.next()) map (a => Option((a, i))) else None.pure[F]
    } yield step
    Stream.unfoldEval(iterator)(getNext)
  }

  def toF[F[_]: Async: ContextShift, A](cf: CompletableFuture[A]): F[A] = {
    if (cf.isDone) cf.get.pure[F]
    else {
      val fa: F[A] = Async[F].async { cb: (Either[Throwable, A] => Unit) =>
        val _ = cf.whenComplete { (res: A, t: Throwable) => {
          val eitherResult: Either[Throwable, A] =
            if (!Option(t).isEmpty) Left(t) else Right(res)
          cb(eitherResult)
        }}
      }
      fa productL ContextShift[F].shift
    }
  }

  def consume[F[_]: Concurrent, K, V](
      stream: Stream[F, IndexedStoreEvent[K, V]],
      store: IndexedStore[F, K, V])
      : F[Deferred[F, Unit]] = {
    def handleEvent(e: IndexedStoreEvent[K, V]): F[Unit] = e match {
      case Insert(k, v) => store.insert(k, v)
      case Remove(k) => store.delete(k) as (())
    }
    for {
      d <- Deferred[F, Unit]
      _ <- Concurrent[F].start(stream.onFinalize(d.complete(())).evalMap(handleEvent).compile.drain)
    } yield d
  }
}
