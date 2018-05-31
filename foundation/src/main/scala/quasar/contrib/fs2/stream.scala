/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.contrib.fs2

import fs2.Stream
import scalaz.MonadPlus

trait StreamInstances {
  implicit def streamMonadPlus[F[_]]: MonadPlus[Stream[F, ?]] =
    new MonadPlus[Stream[F, ?]] {
      def plus[A](x: Stream[F, A], y: => Stream[F, A]) =
        x ++ y

      def empty[A] = Stream.empty

      def bind[A, B](fa: Stream[F, A])(f: A => Stream[F, B]) =
        fa flatMap f

      def point[A](a: => A) =
        Stream.emit(a)
    }
}

object stream extends StreamInstances
