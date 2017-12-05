/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.contrib

import slamdata.Predef._

import _root_.scalaz._, \&/._, Scalaz._

package object scalaz {
  def -\&/[A, B](a: A): These[A, B] = This(a)
  def \&/-[A, B](b: B): These[A, B] = That(b)

  object HasThis {
    def unapply[A](these: A \&/ _): Option[A] = these match {
      case Both(a, _) => a.some
      case This(a)    => a.some
      case That(_)    => none
    }
  }

  object HasThat {
    def unapply[B](these: _ \&/ B): Option[B] = these match {
      case Both(_, b) => b.some
      case This(_)    => none
      case That(b)    => b.some
    }
  }

  def nextM[M[_]: Monad, A, B, C](f: (A, B) => C, m: A => M[B], a: A): M[(B, C)] =
    m(a).map(b => (b, f(a, b)))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def fixpointM[M[_]: Monad, A: Equal](f: A => M[A], a: A): M[A] =
    for {
      a1Changed <- nextM[M, A, A, Boolean](_ ≠ _, f, a)
      (a1, changed) = a1Changed
      a2 <- if (changed) fixpointM(f, a1) else a1.point[M]
    } yield a2

  implicit final class OptionTOps[F[_], A](val self: OptionT[F, A]) extends AnyVal {
    def covary[B >: A](implicit F: Functor[F]): OptionT[F, B] =
      OptionT(self.run.map(opt => opt: Option[B]))
  }

  // It's annoying that Scala doesn't allow us to define this generically for any `MonadTrans`
  // but seems like a nice thing to have on `OptionT` at least
  implicit final class NestedOptionTOps[F[_], A](val self: OptionT[OptionT[F, ?], A]) extends AnyVal {
    def squash(implicit f: Functor[F]): OptionT[F, A] =
      OptionT(self.run.run.map(_.join))
  }

  implicit def toMonadTell_Ops[F[_], W, A](fa: F[A])(implicit F: MonadTell_[F, W]): MonadTell_Ops[F, W, A] =
    new MonadTell_Ops[F, W, A](fa)

  implicit def toMonadError_Ops[F[_], E, A](fa: F[A])(implicit F: MonadError_[F, E]): MonadError_Ops[F, E, A] =
    new MonadError_Ops[F, E, A](fa)
}
