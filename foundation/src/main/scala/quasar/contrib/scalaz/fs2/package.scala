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

import slamdata.Predef._

import _root_.cats.effect.IO

import _root_.fs2.util.{Async, Attempt}
import Async.Ref
import _root_.fs2.{Strategy, Task}

import _root_.scalaz.{~>, Functor}
import _root_.scalaz.syntax.functor._

// import _root_.scalaz.Monad
import shims._

package object fs2 {
  implicit val asyncCatsIO: Async[IO] =
    new Async[IO] {

      def pure[A](a: A): IO[A] = IO.pure(a)

      def attempt[A](fa: IO[A]): IO[Attempt[A]] = fa.attempt
      def fail[A](err: Throwable): IO[A] = IO.raiseError(err)

      def flatMap[A, B](a: IO[A])(f: A => IO[B]): IO[B] = a.flatMap(f)

      def imapRef[F[_], G[_]: Functor: Async, A](ref: Ref[F, A])(to: F ~> G, from: G ~> F): Ref[G, A] = {
        new Ref[G, A] {
          val F: Async[G] = Async[G]

          def access: G[(A, Attempt[A] => G[Boolean])] =
            to(ref.access).map { case (a, f) => (a, f.andThen(to(_))) }

          def set(t: G[A]): G[Unit] = to(ref.set(from(t)))

          override def get: G[A] = to(ref.get)

          def cancellableGet: G[(G[A], G[Unit])] = to(ref.cancellableGet).map {
            case (fa, fu) => (to(fa), to(fu))
          }

        }
      }

      def ref[A]: IO[Ref[IO, A]] = {
        implicit val strat = Strategy.sequential
        IO.async(cb => Async[Task].ref[A].unsafeRunAsync(cb)).map {
          imapRef(_)(λ[Task ~> IO] {
            t => IO.async(cb => t.unsafeRunAsync(cb))
          }, λ[IO ~> Task] {
            i => Task.unforkedAsync(cb => i.unsafeRunAsync(cb))
          })
        }
      }

      def unsafeRunAsync[A](fa: IO[A])(cb: Attempt[A] => Unit): Unit =
        fa.unsafeRunAsync(l => cb(l))

      def suspend[A](fa: => IO[A]): IO[A] = IO.suspend(fa)

    }

}
