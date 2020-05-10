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

import matryoshka.Delay
import matryoshka.data.free._
import matryoshka.patterns.CoEnv

import scalaz.{-\/, \/-, Equal, Free, Functor, Monad, Need}
import scalaz.syntax.equal._

object implicits {
  implicit def lazyEqualEqual[A: LazyEqual]: Equal[A] =
    Equal((x, y) => LazyEqual[A].equal(x, y).value)

  implicit def delayLazyEqual[F[_], A](implicit F: Delay[LazyEqual, F], A: LazyEqual[A])
      : LazyEqual[F[A]] =
    F(A)

  implicit class LazyEqualIdOps(val x: Need[Boolean]) extends scala.AnyVal {
    def && (y: => Need[Boolean]): Need[Boolean] =
      N.bind(x)(b => if (b) y else Need(false))
  }

  implicit def coEnvLazyEqual[E: Equal, F[_]](implicit F: Delay[LazyEqual, F])
      : Delay[LazyEqual, CoEnv[E, F, ?]] =
    new Delay[LazyEqual, CoEnv[E, F, ?]] {
      def apply[A](eql: LazyEqual[A]) =
        LazyEqual.lazyEqual((x, y) => (x.run, y.run) match {
          case (-\/(e1), -\/(e2)) => Need(e1 ≟ e2)
          case (\/-(f1), \/-(f2)) => F(eql).equal(f1, f2)
          case _ => Need(false)
        })
    }

  implicit def freeLazyEqual[F[_]: Functor, A: Equal](implicit F: Delay[LazyEqual, F])
      : LazyEqual[Free[F, A]] =
    LazyEqual.recursive[Free[F, A], CoEnv[A, F, ?]]

  ////

  private val N = Monad[Need]
}
