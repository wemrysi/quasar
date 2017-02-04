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

package quasar.contrib.scalaz

import quasar.Predef._

import scalaz._, Scalaz._

trait StateTInstances extends StateTInstances0 {
  implicit def stateTCatchable[F[_]: Catchable: Monad, S]: Catchable[StateT[F, S, ?]] =
    new Catchable[StateT[F, S, ?]] {
      def attempt[A](fa: StateT[F, S, A]) =
        StateT[F, S, Throwable \/ A](s =>
          Catchable[F].attempt(fa.run(s)) map {
            case -\/(t)       => (s, t.left)
            case \/-((s1, a)) => (s1, a.right)
          })

      def fail[A](t: Throwable) =
        StateT[F, S, A](_ => Catchable[F].fail(t))
    }
}

trait StateTInstances0 {
  implicit def stateTMonadTell[F[_], W, S](implicit F: MonadTell[F, W])
      : MonadTell[StateT[F, S, ?], W] =
    new MonadTell[StateT[F, S, ?], W] {
      def point[A](a: => A) =
        a.point[StateT[F, S, ?]]

      def bind[A, B](fa: StateT[F, S, A])(f: A => StateT[F, S, B]) =
        fa flatMap f

      def writer[A](w: W, v: A): StateT[F, S, A] =
        F.writer(w, v).liftM[StateT[?[_], S, ?]]
    }
}

object stateT extends StateTInstances
