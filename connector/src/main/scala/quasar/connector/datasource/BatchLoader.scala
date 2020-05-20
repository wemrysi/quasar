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

package quasar.connector.datasource

import quasar.connector.Offset

import scala.{Option, None, Product, Serializable}

import cats.{~>, Contravariant, Functor, FlatMap}
import cats.arrow.Strong
import cats.data.Kleisli

sealed trait BatchLoader[F[_], -Q, A] extends Product with Serializable {
  import BatchLoader._

  def andThen[B](f: A => F[B])(implicit F: FlatMap[F]): BatchLoader[F, Q, B] =
    transform(_.andThen(f))

  def compose[P, QQ <: Q](f: P => F[QQ])(implicit F: FlatMap[F]): BatchLoader[F, P, A] =
    transform(_.compose(f))

  def contramap[P](f: P => Q): BatchLoader[F, P, A] =
    transform(_.local(f))

  def loadFull(q: Q): F[A] =
    this match {
      case Full(f) => f(q)
      case Seek(f) => f(q, None)
    }

  def map[B](f: A => B)(implicit F: Functor[F]): BatchLoader[F, Q, B] =
    transform(_.map(f))

  def mapK[G[_]](f: F ~> G): BatchLoader[G, Q, A] =
    transform(_.mapK(f))

  def transform[G[_], P, B](f: Kleisli[F, Q, A] => Kleisli[G, P, B]): BatchLoader[G, P, B] =
    this match {
      case Full(g) => Full(p => f(Kleisli(g)).run(p))
      case Seek(g) => Seek((p, o) => f(Kleisli(g(_, o))).run(p))
    }
}

object BatchLoader {
  final case class Full[F[_], Q, A](load: Q => F[A])
      extends BatchLoader[F, Q, A]

  final case class Seek[F[_], Q, A](load: (Q, Option[Offset]) => F[A])
      extends BatchLoader[F, Q, A]

  implicit def batchLoaderContravariant[F[_]: Functor, B]: Contravariant[BatchLoader[F, *, B]] =
    new Contravariant[BatchLoader[F, *, B]] {
      def contramap[C, D](fb: BatchLoader[F, C, B])(f: D => C): BatchLoader[F, D, B] =
        fb.contramap(f)
    }

  implicit def batchLoaderFunctor[F[_]: Functor, A]: Functor[BatchLoader[F, A, *]] =
    new Functor[BatchLoader[F, A, *]] {
      def map[B, C](fb: BatchLoader[F, A, B])(f: B => C): BatchLoader[F, A, C] =
        fb.map(f)
    }

  implicit def batchLoaderStrongProfunctor[F[_]: Functor]: Strong[BatchLoader[F, *, *]] =
    new Strong[BatchLoader[F, *, *]] {
      def dimap[A, B, C, D](fab: BatchLoader[F, A, B])(f: C => A)(g: B => D): BatchLoader[F, C, D] =
        fab.transform(_.dimap(f)(g))

      def first[A, B, C](fa: BatchLoader[F, A, B]): BatchLoader[F, (A, C), (B, C)] =
        fa.transform(_.first[C])

      def second[A, B, C](fa: BatchLoader[F, A, B]): BatchLoader[F, (C, A), (C, B)] =
        fa.transform(_.second[C])
    }
}
