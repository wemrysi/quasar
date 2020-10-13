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

package quasar.impl.storage.mvstore

import slamdata.Predef._
import quasar.impl.storage.PrefixableStore

import java.util.{Map => JMap}
import scala.collection.JavaConverters._

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._

import fs2.Stream

import org.h2.mvstore._

final class MVPrefixableStore[F[_]: Sync: ContextShift, K, V] private (
    store: MVMap[Array[K], V],
    blocker: Blocker)
    extends PrefixableStore[F, Array[K], V] {

  type Key = Array[K]

  private val db = store.getStore()

  def entries: Stream[F, (Key, V)] =
    Stream.eval(Sync[F].delay(store.entrySet.iterator))
      .flatMap(it => Stream.fromIterator[F](it.asScala))
      .map(jmapEntryTuple)
      .translate(blocker.blockOnK[F])

  def lookup(k: Key): F[Option[V]] =
    blocker.delay(Option(store.get(k)))

  def insert(k: Key, v: V): F[Unit] =
    blocker.delay[F, Unit] {
      store.put(k, v)
      db.commit()
      ()
    }

  def delete(k: Key): F[Boolean] =
    blocker.delay {
      val deleted = Option(store.remove(k)).nonEmpty
      db.commit()
      deleted
    }

  def prefixedEntries(prefix: Key): Stream[F, (Key, V)] = {
    val start = store.ceilingKey(prefix)
    val optStream = for {
      it <- Stream.eval(Sync[F].delay(store.keyIterator(start)))
      key <- Stream.fromIterator[F](it.asScala.takeWhile(_.startsWith(prefix)))
      optV <- Stream.eval(Sync[F].delay(Option(store.get(key))))
    } yield optV.map((key, _))
    optStream.unNone.translate(blocker.blockOnK[F])
  }

  def deletePrefixed(prefix: Key): F[Unit] = for {
    _ <- prefixedEntries(prefix).evalMap(x => blocker.delay(store.remove(x._1))).compile.drain
    _ <- blocker.delay(db.commit())
  } yield ()

  private def jmapEntryTuple[A, B](entry: JMap.Entry[A, B]): (A, B) =
    (entry.getKey, entry.getValue)
}

object MVPrefixableStore {
  def apply[F[_]: Sync: ContextShift, K, V](
      store: MVMap[Array[K], V],
      blocker: Blocker)
      : F[PrefixableStore[F, Array[K], V]] =
    blocker.delay[F, PrefixableStore[F, Array[K], V]] {
      new MVPrefixableStore[F, K, V](store, blocker)
    }
}
