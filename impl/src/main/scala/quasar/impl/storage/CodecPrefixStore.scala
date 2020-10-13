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

import quasar.contrib.scalaz.MonadError_

import cats.Monad
import cats.implicits._

import fs2.Stream

import shapeless._

import scodec._
import scodec.bits.BitVector

final class CodecPrefixStore[F[_]: Monad: MonadError_[?[_], StoreError], K <: HList: LinearCodec, V: Codec] private (
    val underlying: PrefixableStore[F, Array[Byte], Array[Byte]])
    extends PrefixStore[F, K, V] {
  type Constraint[P <: HList] = LinearCodec[P]

  def prefixedEntries[P <: HList: IsPrefix[?, K]: LinearCodec](p: P): Stream[F, (K, V)] =
    underlying.prefixedEntries(encode(p)).evalMap(decodePair)

  def deletePrefixed[P <: HList: IsPrefix[?, K]: LinearCodec](p: P): F[Unit] =
    underlying.deletePrefixed(encode(p))

  def entries: Stream[F, (K, V)] =
    underlying.entries.evalMap(decodePair)

  def lookup(k: K): F[Option[V]] =
    underlying.lookup(encode(k)).flatMap(_.traverse(decodeValue))

  def insert(k: K, v: V): F[Unit] =
    underlying.insert(encode(k), encode(v))

  def delete(k: K): F[Boolean] =
    underlying.delete(encode(k))

  private def encode[A: Codec](a: A): Array[Byte] =
    Codec[A].encode(a) match {
      case Attempt.Successful(bs) => bs.toByteArray
      case _ => quasar.contrib.std.errorImpossible
    }

  private def decode[A: Codec](errMsg: String, bytes: Array[Byte]): F[A] = {
    val bv = BitVector(bytes)
    val F = MonadError_[F, StoreError]
    Codec[A].decode(bv) match {
      case Attempt.Successful(k) =>
        Monad[F].pure(k.value)
      case _ =>
        F.raiseError(StoreError.corrupt(errMsg))
    }
  }

  private def decodeKey(bytes: Array[Byte]): F[K] =
    decode(s"Unable to decode key from '${BitVector(bytes).toHex}'", bytes)

  private def decodeValue(bytes: Array[Byte]): F[V] =
    decode(s"Unable to decode value from '${BitVector(bytes).toHex}'", bytes)

  private def decodePair(pair: (Array[Byte], Array[Byte])): F[(K, V)] = for {
    k <- decodeKey(pair._1)
    v <- decodeValue(pair._2)
  } yield (k, v)
}

object CodecPrefixStore {
  def apply[F[_]: Monad: MonadError_[?[_], StoreError], K <: HList: LinearCodec, V: Codec](
      underlying: PrefixableStore[F, Array[Byte], Array[Byte]])
      : PrefixStore.SCodec[F, K, V] =
    new CodecPrefixStore(underlying)
}
