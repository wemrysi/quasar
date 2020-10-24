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

package quasar.contrib.matryoshka

import scala.Boolean

import matryoshka.{Algebra, Coalgebra, Corecursive, Delay, Recursive}
import matryoshka.data.free._
import matryoshka.implicits.{AlgebraOps, CoalgebraOps}
import matryoshka.patterns.CoEnv

import cats.Eval
import scalaz.{-\/, \/-, Equal, Free, Functor}
import scalaz.Liskov._
import scalaz.syntax.equal._

object implicits {
  implicit final class RecursiveOps[T, F[_]](val t: T)(implicit T: Recursive.Aux[T, F]) {
    def project(implicit F: Functor[F]): F[T] =
      T.project(t)
  }

  implicit final class CorecursiveOps[T, F[_], FF[_]](
      val ft: F[T])(
      implicit T: Corecursive.Aux[T, FF], Sub: F[T] <~< FF[T]) {
    def embed(implicit F: Functor[FF]): T = T.embed(Sub(ft))
  }

  implicit def toAlgebraOps[F[_], A](a: Algebra[F, A]): AlgebraOps[F, A] =
    matryoshka.implicits.toAlgebraOps[F, A](a)

  implicit def toCoalgebraOps[F[_], A](a: Coalgebra[F, A]): CoalgebraOps[F, A] =
    matryoshka.implicits.toCoalgebraOps[F, A](a)

  implicit def lazyEqualEqual[A: LazyEqual]: Equal[A] =
    Equal((x, y) => LazyEqual[A].equal(x, y).value)

  implicit def delayLazyEqual[F[_], A](implicit F: Delay[LazyEqual, F], A: LazyEqual[A])
      : LazyEqual[F[A]] =
    F(A)

  implicit class LazyEqualIdOps(val x: Eval[Boolean]) extends scala.AnyVal {
    def && (y: => Eval[Boolean]): Eval[Boolean] =
      x.flatMap(b => if (b) y else Eval.now(false))
  }

  implicit def coEnvLazyEqual[E: Equal, F[_]](implicit F: Delay[LazyEqual, F])
      : Delay[LazyEqual, CoEnv[E, F, ?]] =
    new Delay[LazyEqual, CoEnv[E, F, ?]] {
      def apply[A](eql: LazyEqual[A]) =
        LazyEqual.lazyEqual((x, y) => (x.run, y.run) match {
          case (-\/(e1), -\/(e2)) => Eval.now(e1 ≟ e2)
          case (\/-(f1), \/-(f2)) => F(eql).equal(f1, f2)
          case _ => Eval.now(false)
        })
    }

  implicit def freeLazyEqual[F[_]: Functor, A: Equal](implicit F: Delay[LazyEqual, F])
      : LazyEqual[Free[F, A]] =
    LazyEqual.recursive[Free[F, A], CoEnv[A, F, ?]]
}
