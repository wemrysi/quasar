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

import quasar.impl.storage.{IsPrefix, PrefixStore}

import scala.collection.JavaConverters._
import java.util.{Map => JMap}

import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._

import fs2.Stream

import org.h2.mvstore._

import shapeless._
import shapeless.ops.hlist._
import shapeless.ops.traversable._

import scodec.codecs._
import scodec.bits.BitVector
import scodec._

final class MVStorePrefixStore[F[_]: Sync: ContextShift, K <: HList: Codec, V: Codec] (
    store: MVMap[Array[Byte], Array[Byte]],
    blocker: Blocker)
    extends PrefixStore[F, K, V] {

  type Constraint[P <: HList] = Codec[P]

  lazy val db = store.getStore

  def prefixedEntries[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      pCodec: Codec[P])
      : Stream[F, (K, V)] = {
    val arr = encode(p)
    val optStream = for {
      start <- Stream.eval(Sync[F].delay(store.ceilingKey(arr)))
      arrKey <- prefixedKeys(p)
      key <- Stream.eval(mkKey(arrKey))
      arrV <- Stream.eval(Sync[F].delay(Option(store.get(arrKey))))
      optV <- Stream.eval(arrV.traverse(mkValue))
    } yield optV.map((key, _))
    optStream.unNone.translate(blocker.blockOnK[F])
  }

  def deletePrefixed[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      pCodec: Codec[P])
      : F[Unit] = for {
    _ <- prefixedKeys(p).evalMap(arr => Sync[F].delay(store.remove(arr))).compile.drain
    _ <- commit
  } yield ()

  def entries: Stream[F, (K, V)] = {
    Stream.eval(Sync[F].delay(store.entrySet.iterator))
      .flatMap(it => Stream.fromIterator[F](it.asScala))
      .evalMap(mkPair)
      .translate(blocker.blockOnK[F])
  }

  def lookup(k: K): F[Option[V]] =
    blocker.delay(Option(store.get(encode(k)))).flatMap(_.traverse(mkValue(_)))

  def insert(k: K, v: V): F[Unit] =
    blocker.delay[F, Unit](store.put(encode(k), encode(v))) >> commit

  private def commit: F[Unit] = blocker.delay(db.commit()).void

  def delete(k: K): F[Boolean] =
    blocker.delay(Option(store.remove(encode(k))).nonEmpty)

  private def prefixedKeys[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      pCodec: Codec[P])
      : Stream[F, Array[Byte]] = {
    val arrP = encode(p)
    val start = store.ceilingKey(arrP)
    Stream.eval(Sync[F].delay(store.keyIterator(start)))
      .flatMap(it => Stream.fromIterator[F](it.asScala.takeWhile(_.startsWith(arrP))))

  }

  private def encode[A: Codec](k: A): Array[Byte] =
    Codec[A].encode(k) match {
      case Attempt.Successful(bs) =>
        bs.toByteArray
      case _ =>
        new Array(0)
    }

  private def mkKey(bytes: Array[Byte]): F[K] = {
    val bv = BitVector(bytes)
    Codec[K].decode(bv) match {
      case Attempt.Successful(k) =>
        Sync[F].pure(k.value)
      case _ =>
        Sync[F].raiseError[K](new RuntimeException(
          s"Unable to decode key from '${bv.toHex}'"))
    }
  }

  private def mkValue(bytes: Array[Byte]): F[V] = {
    val bv = BitVector(bytes)
    Codec[V].decode(bv) match {
      case Attempt.Successful(k) =>
        Sync[F].pure(k.value)
      case _ =>
        Sync[F].raiseError[V](new RuntimeException(
          s"Unable to decode value from '${bv.toHex}'"))
    }
  }

  private def mkPair(entry: JMap.Entry[Array[Byte], Array[Byte]]): F[(K, V)] = for {
    k <- mkKey(entry.getKey)
    v <- mkValue(entry.getValue)
  } yield (k, v)
}

object MVStorePrefixStore {
  def apply[F[_]: Sync: ContextShift, K <: HList: Codec, V: Codec](
      db: MVStore,
      name: String,
      blocker: Blocker)
      : F[PrefixStore.SCodec[F, K, V]] =
    blocker.delay[F, PrefixStore.SCodec[F, K, V]] {
      val store: MVMap[Array[Byte], Array[Byte]] = db.openMap(name)
      new MVStorePrefixStore[F, K, V](store, blocker)
    }

}
