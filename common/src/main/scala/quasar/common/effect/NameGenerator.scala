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

package quasar.common.effect

import slamdata.Predef._

import quasar.contrib.scalaz.MonadState_
import quasar.fp.ski.κ

import cats.data.StateT

import simulacrum.typeclass

import scalaz.{StateT => _, _}
import scalaz.syntax.bind._

import shims.monadToCats

/** A source of strings unique within `F[_]`, an implementation must have the
  * property that, if Applicative[F], then (freshName |@| freshName)(_ != _).
  */
@typeclass trait NameGenerator[F[_]] {
  /** Returns a fresh name, guaranteed to be unique among all the other names
    * generated from `F`.
    */
  def freshName: F[String]

  /** Returns a fresh name, prefixed with the given string. */
  def prefixedName(prefix: String)(implicit F: Functor[F]): F[String] =
    freshName map (prefix + _)
}

object NameGenerator extends NameGeneratorInstances

sealed abstract class NameGeneratorInstances extends NameGeneratorInstances0 {
  implicit def sequenceNameGenerator[F[_]: Monad](implicit F: MonadState_[F, Long]): NameGenerator[F] =
    new NameGenerator[F] {
      def freshName = F.get.flatMap(n => F.put(n + 1) as n.toString)
    }
}

sealed abstract class NameGeneratorInstances0 {
  implicit def eitherTNameGenerator[F[_]: NameGenerator : Functor, A]: NameGenerator[EitherT[F, A, ?]] =
    new NameGenerator[EitherT[F, A, ?]] {
      def freshName = EitherT.rightT(NameGenerator[F].freshName)
    }

  implicit def readerTNameGenerator[F[_]: NameGenerator, A]: NameGenerator[ReaderT[F, A, ?]] =
    new NameGenerator[ReaderT[F, A, ?]] {
      def freshName = ReaderT(κ(NameGenerator[F].freshName))
    }

  implicit def catsStateTNameGenerator[F[_]: NameGenerator : Monad, S]: NameGenerator[StateT[F, S, ?]] =
    new NameGenerator[StateT[F, S, ?]] {
      def freshName = StateT(s => NameGenerator[F].freshName strengthL s)
    }

  implicit def writerTNameGenerator[F[_]: NameGenerator : Functor, W: Monoid]: NameGenerator[WriterT[F, W, ?]] =
    new NameGenerator[WriterT[F, W, ?]] {
      def freshName = WriterT.put(NameGenerator[F].freshName)(Monoid[W].zero)
    }
}
