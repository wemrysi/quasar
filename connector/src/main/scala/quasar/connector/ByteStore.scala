/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.connector

import slamdata.Predef._

import quasar.higher.HFunctor

import java.lang.String
import java.nio.charset.Charset

import cats.{Applicative, Functor}
import cats.effect.Concurrent
import cats.effect.concurrent.MVar
import cats.implicits._

import scalaz.~>

trait ByteStore[F[_]] {
  /** Retrieve the bytes associated with the given key. */
  def lookup(key: String): F[Option[Array[Byte]]]

  /** Associate `bytes` with the specified key, replaces any
    * existing association.
    */
  def insert(key: String, bytes: Array[Byte]): F[Unit]

  /** Remove any association with the given key, returns whether an association
    * existed.
    */
  def delete(key: String): F[Boolean]
}

object ByteStore {

  /* An in-memory `ByteStore` without any persistence. */
  def ephemeral[F[_]: Concurrent]: F[ByteStore[F]] =
    MVar.of(Map.empty[String, Array[Byte]]) map { mvar =>
      new ByteStore[F] {
        def lookup(key: String): F[Option[Array[Byte]]] =
          mvar.read.map(_.get(key))

        def insert(key: String, bytes: Array[Byte]): F[Unit] =
          for {
            store <- mvar.take
            _ <- mvar.put(store + (key -> bytes))
          } yield ()

        def delete(key: String): F[Boolean] =
          for {
            store <- mvar.take
            _ <- mvar.put(store - key)
          } yield store.contains(key)
      }
    }

  /* An empty `ByteStore` that ignores all inserts. */
  def void[F[_]: Applicative]: ByteStore[F] =
    new ByteStore[F] {
      def lookup(key: String): F[Option[Array[Byte]]] = none[Array[Byte]].pure[F]
      def insert(key: String, bytes: Array[Byte]): F[Unit] = ().pure[F]
      def delete(key: String): F[Boolean] = false.pure[F]
    }

  object ops {
    private lazy val utf8 = Charset.forName("UTF-8")

    final implicit class ByteStoreOps[F[_]](val bs: ByteStore[F]) extends scala.AnyVal {
      def lookupString(key: String)(implicit F: Functor[F]): F[Option[String]] =
        F.map(bs.lookup(key))(_.map(new String(_, utf8)))

      def insertString(key: String, value: String): F[Unit] =
        bs.insert(key, value.getBytes(utf8))
    }
  }

  implicit val byteStoreHFunctor: HFunctor[ByteStore] =
    new HFunctor[ByteStore] {
      def hmap[A[_], B[_]](fa: ByteStore[A])(f: A ~> B): ByteStore[B] =
        new ByteStore[B] {
          def lookup(key: String): B[Option[Array[Byte]]] = f(fa.lookup(key))
          def insert(key: String, bytes: Array[Byte]): B[Unit] = f(fa.insert(key, bytes))
          def delete(key: String): B[Boolean] = f(fa.delete(key))
        }
    }
}
