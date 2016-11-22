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

import scalaz.MonadTell

/** A version of MonadTell that doesn't extend Monad to avoid ambiguous implicits
  * in the presence of multiple "mtl" constraints.
  */
trait MonadTell_[F[_], S] { self =>
  def MT: MonadTell[F, S]

  def writer[A](w: S, v: A): F[A]
}

object MonadTell_ {
  def apply[F[_], E](implicit F: MonadTell_[F, E]): MonadTell_[F, E] = F

  implicit def monadTellNoMonad[F[_], E](implicit F: MonadTell[F, E])
      : MonadTell_[F, E] =
    new MonadTell_[F, E] {
      def MT = F

      def writer[A](w: E, v: A): F[A] = F.writer(w, v)
    }
}

final class MonadTell_Ops[F[_], S, A] private[scalaz](self: F[A])(implicit val F: MonadTell_[F, S]) {
  final def :++>(w: ⇒ S): F[A] =
    F.MT.bind(self)(a => F.MT.map(F.MT.tell(w))(_ => a))

  final def :++>>(f: (A) ⇒ S): F[A] =
    F.MT.bind(self)(a => F.MT.map(F.MT.tell(f(a)))(_ => a))
}
