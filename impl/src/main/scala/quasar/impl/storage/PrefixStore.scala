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
  def prefixedEntries[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      toArray: ToTraversable.Aux[P, Array, AnyRef])
      : Stream[F, (K, V)]

  def deletePrefixed[P <: HList](p: P)(
      implicit
      pfx: IsPrefix[P, K],
      toArray: ToTraversable.Aux[P, Array, AnyRef])
      : F[Unit]
}

object PrefixStore {
  def xmapValueF[F[_]: Monad, K <: HList, V1, V2](
      s: PrefixStore[F, K, V1])(
      f: V1 => F[V2])(
      g: V2 => F[V1])
      : PrefixStore[F, K, V2] =
    new PrefixStore[F, K, V2] {
      def prefixedEntries[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: ToTraversable.Aux[P, Array, AnyRef])
          : Stream[F, (K, V2)] =
        s.prefixedEntries(p).evalMap(_.traverse(f))

      def deletePrefixed[P <: HList](p: P)(
          implicit
          pfx: IsPrefix[P, K],
          toArray: ToTraversable.Aux[P, Array, AnyRef])
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
