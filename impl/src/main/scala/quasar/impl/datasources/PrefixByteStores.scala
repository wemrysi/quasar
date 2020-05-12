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

package quasar.impl.datasources

import quasar.connector.ByteStore
import quasar.impl.storage.PrefixStore

import java.lang.String
import scala.{AnyRef, Array, Boolean, Byte, Option, Unit}

import cats.Applicative

import shapeless._

final class PrefixByteStores[F[_]: Applicative, K <: AnyRef] private (
    store: PrefixStore[F, K :: String :: HNil, Array[Byte]])
    extends ByteStores[F, K] {

  private class PrefixByteStore(prefix: K) extends ByteStore[F] {
    def lookup(key: String): F[Option[Array[Byte]]] =
      store.lookup(prefix :: key :: HNil)

    def insert(key: String, value: Array[Byte]): F[Unit] =
      store.insert(prefix :: key :: HNil, value)

    def delete(key: String): F[Boolean] =
      store.delete(prefix :: key :: HNil)
  }

  def get(prefix: K): F[ByteStore[F]] =
    Applicative[F].pure(new PrefixByteStore(prefix))

  def clear(prefix: K): F[Unit] =
    store.deletePrefixed(prefix :: HNil)
}

object PrefixByteStores {
  def apply[F[_]: Applicative, K <: AnyRef](
      store: PrefixStore[F, K :: String :: HNil, Array[Byte]])
      : ByteStores[F, K] =
    new PrefixByteStores(store)
}
