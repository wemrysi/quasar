/*
 * Copyright 2014–2020 SlamData Inc.
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
import cats.syntax.functor._

sealed trait BatchLoader[F[_], -Q, A] extends Product with Serializable {
  import BatchLoader._

  def contramap[P](f: P => Q): BatchLoader[F, P, A] =
    this match {
      case Full(g) => Full(g compose f)
    }

  def map[B](f: A => B)(implicit F: Functor[F]): BatchLoader[F, Q, B] =
    this match {
      case Full(g) => Full(q => g(q).map(f))
    }

  def mapK[G[_]](f: F ~> G): BatchLoader[G, Q, A] =
    this match {
      case Full(g) => Full((f[A] _) compose g)
    }
}

object BatchLoader {
  final case class Full[F[_], Q, A](load: Q => F[A])
      extends BatchLoader[F, Q, A]
}
