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

package quasar.contrib.cats.effect

import quasar.contrib.scalaz.MonadTell_

import cats.Applicative
import cats.effect.Resource

object resource {
  implicit def catsEffectResourceMonadTell_[F[_]: Applicative, W](
      implicit F: MonadTell_[F, W])
      : MonadTell_[Resource[F, ?], W] =
    new MonadTell_[Resource[F, ?], W] {
      def writer[A](w: W, a: A) = Resource.liftF(F.writer(w, a))
    }
}
