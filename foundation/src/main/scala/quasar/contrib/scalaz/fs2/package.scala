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

package quasar.contrib

import slamdata.Predef.Throwable

import _root_.cats.effect.IO

import _root_.fs2.util.{Attempt, Catchable}

// import _root_.scalaz.Monad

package object fs2 {
  implicit val catchableCatsIO: Catchable[IO] =
    new Catchable[IO] {

      def pure[A](a: A): IO[A] = IO.pure(a)

      def attempt[A](fa: IO[A]): IO[Attempt[A]] = fa.attempt
      def fail[A](err: Throwable): IO[A] = IO.raiseError(err)

      def flatMap[A, B](a: IO[A])(f: A => IO[B]): IO[B] = a.flatMap(f)

    }

  // implicit val monadCatsIO: Monad[IO] =
  //   new Monad[IO] {
  //     def point[A](a: => A): IO[A] = IO.pure(a)
  //     def bind[A, B](a: IO[A])(f: A => IO[B]): IO[B] = a.flatMap(f)
  //   }
}
