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

import cats.Applicative

import fs2.Stream

import shapeless._
import shapeless.ops.hlist._

object VoidStore {
  def prefix[F[_], K <: HList, V](implicit F: Applicative[F]): PrefixStore.TStore[F, K, V] =
    new PrefixStore[F, K, V] {
      type Constraint[P <: HList] = PrefixStore.ToArray[P]

      def prefixedEntries[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: ToTraversable.Aux[P, Array, AnyRef])
          : Stream[F, (K, V)] =
        Stream.empty

      def deletePrefixed[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: ToTraversable.Aux[P, Array, AnyRef])
          : F[Unit] =
        F.unit

      val entries: Stream[F, (K, V)] = Stream.empty
      def insert(k: K, v: V): F[Unit] = F.unit
      def lookup(k: K): F[Option[V]] = F.pure(None: Option[V])
      def delete(k: K): F[Boolean] = F.pure(false)
    }
}
