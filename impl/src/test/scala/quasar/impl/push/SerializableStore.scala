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

package quasar.impl.push

import slamdata.Predef._

import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage.{PrefixStore, LinearCodec, StoreError, IsPrefix}
import quasar.impl.storage.mvstore._

import cats.Monad
import cats.effect.{Blocker, ContextShift, Sync}
import cats.implicits._

import fs2.Stream

import org.h2.mvstore._
import org.h2.mvstore.`type`.ObjectDataType

import shapeless._

import scodec.{Attempt, Codec}
import scodec.bits.BitVector
import scodec.codecs._

object SerializableStore {
  // The same as MVPrefixStore, but no codecs are used for value
  // Unfortunately `codec` from this module doesn't work (as well as based on mapdb or just plain java serializer)
  // for MVPrefixStore, most likely due to class loader issues in sbt test runner.
  def apply[F[_]: MonadError_[?[_], StoreError]: Sync: ContextShift, K <: HList: LinearCodec, V](
      db: MVStore,
      name: String,
      blocker: Blocker)
      : F[PrefixStore.SCodec[F, K, V]] = {

    val builder = (new MVMap.Builder[Array[Byte], V]()).keyType(ByteArrayDataType)
    val store = db.openMap(name, builder)

    MVPrefixableStore[F, Byte, V](store, blocker).map { underlying =>
      new PrefixStore[F, K, V] {
        type Constraint[P <: HList] = LinearCodec[P]

        def prefixedEntries[P <: HList: IsPrefix[?, K]: LinearCodec](p: P): Stream[F, (K, V)] =
          underlying.prefixedEntries(encode(p)).evalMap(decodePair)

        def deletePrefixed[P <: HList: IsPrefix[?, K]: LinearCodec](p: P): F[Unit] =
          underlying.deletePrefixed(encode(p))

        def entries: Stream[F, (K, V)] =
          underlying.entries.evalMap(decodePair)

        def lookup(k: K): F[Option[V]] =
          underlying.lookup(encode(k))

        def insert(k: K, v: V): F[Unit] =
          underlying.insert(encode(k), v)

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

        private def decodePair(pair: (Array[Byte], V)): F[(K, V)] =
          pair.leftTraverse(decodeKey)
      }
    }
  }

  def codec[A <: Serializable]: Codec[A] =
    variableSizeBits(int32, bits).xmap(fromArray(_).asInstanceOf[A], toArray)

  private def toArray(a: Serializable): BitVector =
    BitVector(ObjectDataType.serialize(a))

  private def fromArray(a: BitVector): Any =
    ObjectDataType.deserialize(a.toByteArray)
}
