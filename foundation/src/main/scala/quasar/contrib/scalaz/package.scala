/*
 * Copyright 2014–2016 SlamData Inc.
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

import _root_.scalaz.{Monad, MonadError, MonadTell, StateT}
import _root_.scalaz.syntax.monad._

package object scalaz {
  implicit def toMonadError[F[_], E](me: MonadError_[F, E]): MonadError[F, E] =
    me.ME

  implicit def toMonadError_Ops[F[_], S, A](v: F[A])(implicit F0: MonadError_[F, S]) =
    new MonadError_Ops[F, S, A](v)

  implicit def stateTMonadError[F[_], E, S](implicit F: MonadError[F, E])
      : MonadError[StateT[F, S, ?], E] =
    new MonadError[StateT[F, S, ?], E] {
      def point[A](a: => A) = a.point[F].liftM[StateT[?[_], S, ?]]

      def bind[A, B]
        (fa: StateT[F, S, A])
        (f: A => StateT[F, S, B]) =
        StateT[F, S, B](s =>
          F.bind(fa.run(s))(p => f(p._2).run(p._1)))

      def handleError[A]
        (fa: StateT[F, S, A])
        (f: E => StateT[F, S, A]) =
        StateT[F, S, A](s => F.handleError(fa.run(s))(f(_).run(s)))

      def raiseError[A](e: E) =
        StateT[F, S, A](_ => F.raiseError[(S, A)](e))
    }

  implicit def stateTMonadError_[F[_], E, S](implicit F: MonadError_[F, E])
      : MonadError_[StateT[F, S, ?], E] =
    new MonadError_[StateT[F, S, ?], E] {
      def ME = stateTMonadError(F.ME)

      implicit val FM: Monad[F] = F.ME

      def handleError[A]
        (fa: StateT[F, S, A])
        (f: E => StateT[F, S, A]) =
        StateT[F, S, A](s => F.handleError(fa.run(s))(f(_).run(s)))

      def raiseError[A](e: E) =
        StateT[F, S, A](_ => F.raiseError[(S, A)](e))
    }

  implicit def stateTMonadTell[F[_], W, S](implicit F: MonadTell[F, W])
      : MonadTell[StateT[F, S, ?], W] =
    new MonadTell[StateT[F, S, ?], W] {
      def point[A](a: => A) = a.point[F].liftM[StateT[?[_], S, ?]]

      def bind[A, B]
        (fa: StateT[F, S, A])
        (f: A => StateT[F, S, B]) =
        StateT[F, S, B](s =>
          F.bind(fa.run(s))(p => f(p._2).run(p._1)))

      def writer[A](w: W, v: A): StateT[F, S, A] =
        F.writer(w, v).liftM[StateT[?[_], S, ?]]
    }
}
