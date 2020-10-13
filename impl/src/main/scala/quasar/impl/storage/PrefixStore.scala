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

import scala.{AnyRef, Array, Boolean, Option, Unit}

import cats.Monad
import cats.implicits._

import fs2.Stream

import shapeless._
import shapeless.ops.hlist._

trait PrefixStore[F[_], K <: HList, V] extends IndexedStore[F, K, V] {
  type Constraint[P <: HList]

  def prefixedEntries[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      constraint: Constraint[P])
      : Stream[F, (K, V)]

  def deletePrefixed[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      constraint: Constraint[P])
      : F[Unit]
}

object PrefixStore {
  type ToArray[L <: HList] = ToTraversable.Aux[L, Array, AnyRef]

  type Aux[F[_], K <: HList, V, C[P <: HList]] = PrefixStore[F, K, V] {
    type Constraint[P <: HList] = C[P]
  }

  type Legacy[F[_], K <: HList, V] = Aux[F, K, V, ToArray]
  type SCodec[F[_], K <: HList, V] = Aux[F, K, V, LinearCodec]

  def xmapValueF[F[_]: Monad, K <: HList, V1, V2](
      s: PrefixStore[F, K, V1])(
      f: V1 => F[V2])(
      g: V2 => F[V1])
      : PrefixStore.Aux[F, K, V2, s.Constraint] =
    new PrefixStore[F, K, V2] {
      type Constraint[P <: HList] = s.Constraint[P]

      def prefixedEntries[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: Constraint[P])
          : Stream[F, (K, V2)] =
        s.prefixedEntries(p).evalMap(_.traverse(f))

      def deletePrefixed[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: Constraint[P])
          : F[Unit] =
        s.deletePrefixed(p)

      def entries: Stream[F, (K, V2)] =
        s.entries.evalMap(_.traverse(f))

      def lookup(k: K): F[Option[V2]] =
        s.lookup(k).flatMap(_.traverse(f))

      def insert(k: K, v2: V2): F[Unit] =
        g(v2).flatMap(s.insert(k, _))

      def delete(k: K): F[Boolean] =
        s.delete(k)
    }

}
