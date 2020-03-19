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

import scala.{Product, Serializable}

import cats.{~>, Functor}

sealed trait Loader[F[_], -Q, A] extends Product with Serializable {
  def contramap[P](f: P => Q): Loader[F, P, A] =
    this match {
      case Loader.Batch(b) => Loader.Batch(b contramap f)
    }

  def map[B](f: A => B)(implicit F: Functor[F]): Loader[F, Q, B] =
    this match {
      case Loader.Batch(b) => Loader.Batch(b map f)
    }

  def mapK[G[_]](f: F ~> G): Loader[G, Q, A] =
    this match {
      case Loader.Batch(b) => Loader.Batch(b mapK f)
    }
}

object Loader {
  final case class Batch[F[_], Q, A](batchLoader: BatchLoader[F, Q, A])
      extends Loader[F, Q, A]
}
