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

package quasar.contrib.scalaz

import slamdata.Predef._

import cats.effect._

import scala.util.{Failure, Success}

import scalaz.{-\/, \/-} // \/}
import scalaz.concurrent.{Strategy, Task}
import scalaz.syntax.apply._

package object concurrent {

  // cribbed from cats-effect
  def shift(implicit S: Strategy): Task[Unit] = {
    Task async { cb =>
      val _ = S(cb(\/-(())))

      ()
    }
  }

  implicit def taskEffect: Effect[Task] = new Effect[Task] {
    def pure[A](x: A): Task[A] = Task.now(x)
    def handleErrorWith[A](fa: Task[A])(f: Throwable => Task[A]): Task[A] =
      // fa.attempt.flatMap(_.fold(Task.now, f))
      Task.fail(new Exception())

    def raiseError[A](e: Throwable): Task[A] = Task.fail(e)
    def async[A](k: (Either[Throwable, A] => Unit) => Unit): Task[A] =
      // Task.async(r => k(r))
      Task.fail(new Exception())
    def bracketCase[A, B](acquire: Task[A])(use: A => Task[B])(release: (A, ExitCase[Throwable]) => Task[Unit]): Task[B] = for {
      res <- acquire
      u <- use(res).attempt.flatMap {
        case -\/(err) => release(res, ExitCase.Error(err)) *> Task.fail(err)
        case \/-(a) => release(res, ExitCase.Completed) *> Task.now(a)
      }
    } yield u
    def runAsync[A](fa: Task[A])(cb: Either[Throwable, A] => IO[Unit]): IO[Unit] = IO(fa.unsafePerformAsync {
      e => cb(e.toEither).unsafeRunAsync(_ => ())
    })
    def flatMap[A, B](fa: Task[A])(f: A => Task[B]): Task[B] = fa.flatMap(f)
    def tailRecM[A, B](a: A)(f: A => Task[Either[A, B]]): Task[B] = // f(a).flatMap(_.fold(tailRecM(_)(f), Task.now))
      Task.fail(new Exception())
    def suspend[A](thunk: => Task[A]): Task[A] = Task.suspend(thunk)
  }

  def runToIO[F[_], A](fa: F[A])(implicit F: Effect[F]): IO[A] = IO.async[A] {
    (cb: Either[Throwable, A] => Unit) =>
      val prom = scala.concurrent.Promise[A]()
        F.runAsync(fa)((eta: Throwable Either A) =>
          Effect[IO].map2(
            IO(cb(eta)),
            IO(prom.complete(eta.fold(Failure(_), Success(_))))
          )((_, _) => ())
        ).unsafeRunAsync(
          _ => ()
        )
  }
}
