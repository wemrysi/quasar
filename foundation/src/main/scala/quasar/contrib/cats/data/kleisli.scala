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

package quasar.contrib.cats.data

import quasar.contrib.scalaz.{MonadError_, MonadTell_}

import cats.data.Kleisli

object kleisli {
  implicit def catsDataKleisliMonadError_[E, F[_]: MonadError_[?[_], E], A]
      : MonadError_[Kleisli[F, A, ?], E] =
    new MonadError_[Kleisli[F, A, ?], E] {
      def raiseError[B](e: E): Kleisli[F, A, B] =
        Kleisli(_ => MonadError_[F, E].raiseError[B](e))

      def handleError[B](fb: Kleisli[F, A, B])(f: E => Kleisli[F, A, B]): Kleisli[F, A, B] =
        Kleisli(a => MonadError_[F, E].handleError(fb(a))(e => f(e)(a)))
    }

  implicit def catsDataKleisliMonadTell[W, F[_]: MonadTell_[?[_], W], A]
      : MonadTell_[Kleisli[F, A, ?], W] =
    new MonadTell_[Kleisli[F, A, ?], W] {
      def writer[B](w: W, b: B): Kleisli[F, A, B] =
        Kleisli(_ => MonadTell_[F, W].writer(w, b))
    }
}
