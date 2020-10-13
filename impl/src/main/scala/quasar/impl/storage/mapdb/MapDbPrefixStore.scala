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

package quasar.impl.storage.mapdb

import slamdata.Predef._

import quasar.impl.storage.{IsPrefix, PrefixStore}

import scala.collection.JavaConverters._
import java.util.{Map => JMap}

import cats.effect.{Blocker, ContextShift, Sync}
import cats.syntax.functor._

import fs2.Stream

import org.mapdb.{BTreeMap, DB, Serializer}
import org.mapdb.serializer.{GroupSerializer, SerializerArrayTuple}

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.traversable._

final class MapDbPrefixStore[F[_]: Sync: ContextShift, K <: HList, V] private (
    db: DB,
    store: BTreeMap[Array[AnyRef], V],
    blocker: Blocker)(
    implicit
    kToArray: ToTraversable.Aux[K, Array, AnyRef],
    kFromTraversable: FromTraversable[K])
    extends PrefixStore[F, K, V] {

  type Constraint[P <: HList] = PrefixStore.ToArray[P]

  def prefixedEntries[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      pToArray: ToTraversable.Aux[P, Array, AnyRef])
      : Stream[F, (K, V)] =
    entriesStream(store.prefixSubMap(pToArray(p)))

  def deletePrefixed[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      pToArray: ToTraversable.Aux[P, Array, AnyRef])
      : F[Unit] =
    blocker.delay[F, Unit] {
      store.prefixSubMap(pToArray(p)).clear()
      db.commit()
    }

  val entries: Stream[F, (K, V)] =
    entriesStream(store)

  def lookup(k: K): F[Option[V]] =
    blocker.delay(Option(store.get(kToArray(k))))

  def insert(k: K, v: V): F[Unit] =
    blocker.delay[F, Unit] {
      store.put(kToArray(k), v)
      db.commit()
    }

  def delete(k: K): F[Boolean] =
    blocker.delay(Option(store.remove(kToArray(k))).nonEmpty)

  ////

  private def entriesStream(m: JMap[Array[AnyRef], V]): Stream[F, (K, V)] =
    Stream.eval(Sync[F].delay(m.entrySet.iterator))
      .flatMap(it => Stream.fromIterator[F](it.asScala))
      .evalMap(e => mkKey(e.getKey).tupleRight(e.getValue))
      .translate(blocker.blockOnK[F])

  private def mkKey(parts: Array[AnyRef]): F[K] =
    kFromTraversable(parts) match {
      case Some(k) =>
        Sync[F].pure(k)

      case None =>
        Sync[F].raiseError[K](new RuntimeException(
          s"Unable to decode key from '${parts.mkString("[", ", ", "]")}'"))
    }
}

object MapDbPrefixStore {
  // If instantiation fails due to lack of FromTraversable, ensure
  // scala.Predef.classOf is in scope.
  def apply[F[_]]: PartiallyApplied[F] =
    new PartiallyApplied[F]

  final class PartiallyApplied[F[_]] {
    def apply[SS <: HList, V, K <: HList, S[X] <: Serializer[X]](
        name: String,
        db: DB,
        keySerializer: SS,
        valueSerializer: GroupSerializer[V],
        blocker: Blocker)(
        implicit
        sync: Sync[F],
        contextShift: ContextShift[F],
        ssComapped: Comapped.Aux[SS, S, K],
        ssToList: ToTraversable.Aux[SS, List, S[_]],
        kToArray: ToTraversable.Aux[K, Array, AnyRef],
        kFromTraversable: FromTraversable[K])
        : F[PrefixStore.Legacy[F, K, V]] =
      blocker.delay[F, PrefixStore.Legacy[F, K, V]] {
        val store =
          db.treeMap(name)
            .keySerializer(new SerializerArrayTuple(ssToList(keySerializer): _*))
            .valueSerializer(valueSerializer)
            .createOrOpen()

        new MapDbPrefixStore[F, K, V](db, store, blocker)
      }
  }
}
