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

import slamdata.Predef.{Throwable, Unit}

import scala.util.Either

import cats.effect.{ExitCase, IO, LiftIO, Sync}
import fs2.Stream
import scalaz.ApplicativePlus

trait StreamInstances {
  implicit def streamLiftIO[F[_]: LiftIO]: LiftIO[Stream[F, ?]] =
    new LiftIO[Stream[F, ?]] {
      def liftIO[A](ioa: IO[A]): Stream[F, A] =
        Stream.eval(LiftIO[F].liftIO(ioa))
    }

  implicit def streamApplicativePlus[F[_]]: ApplicativePlus[Stream[F, ?]] =
    new ApplicativePlus[Stream[F, ?]] {
      def plus[A](x: Stream[F, A], y: => Stream[F, A]) =
        x ++ y

      def empty[A] = Stream.empty

      def ap[A, B](fa: => Stream[F, A])(ff: => Stream[F, A => B]) =
        for {
          a <- fa
          f <- ff
        } yield f(a)

      def point[A](a: => A) =
        Stream.emit(a)

      override def map[A, B](fa: Stream[F, A])(f: A => B) =
        fa map f
    }

  implicit def streamSync[F[_]](implicit F: Sync[F]): Sync[Stream[F, ?]] =
    new Sync[Stream[F, ?]] {
      val ME = Stream.monadErrorInstance[F]

      def suspend[A](fa: => Stream[F, A]): Stream[F, A] =
        ME.flatten(delay(fa))

      override def delay[A](a: => A): Stream[F, A] =
        Stream.eval(F.delay(a))

      def bracketCase[A, B]
          (acq: Stream[F, A])
          (use: A => Stream[F, B])
          (rel: (A, ExitCase[Throwable]) => Stream[F, Unit])
          : Stream[F, B] =
        acq flatMap { a =>
          use(a)
            .handleErrorWith(t => rel(a, ExitCase.error(t)).drain)
            .append(rel(a, ExitCase.complete).drain)
        }

      def pure[A](x: A): Stream[F, A] =
        ME.pure(x)

      def handleErrorWith[A](fa: Stream[F, A])(f: Throwable => Stream[F, A]): Stream[F,A] =
        ME.handleErrorWith(fa)(f)

      def raiseError[A](e: Throwable): Stream[F, A] =
        ME.raiseError(e)

      def flatMap[A, B](fa: Stream[F, A])(f: A => Stream[F, B]): Stream[F, B] =
        ME.flatMap(fa)(f)

      def tailRecM[A, B](a: A)(f: A => Stream[F, Either[A, B]]): Stream[F, B] =
        ME.tailRecM(a)(f)
    }


}

object stream extends StreamInstances
