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

import matryoshka.{Delay, Recursive}

import scalaz.{Functor, Need}
import scalaz.syntax.bind._

trait LazyEqual[A] {
  def equal(x: A, y: A): Need[Boolean]
}

object LazyEqual {
  def apply[A](implicit ev: LazyEqual[A]): LazyEqual[A] = ev

  def lazyEqual[A](f: (A, A) => Need[Boolean]): LazyEqual[A] =
    new LazyEqual[A] {
      def equal(x: A, y: A) = f(x, y)
    }

  def recursive[T, F[_]: Functor](implicit T: Recursive.Aux[T, F], F: Delay[LazyEqual, F]): LazyEqual[T] =
    lazyEqual((x, y) => Need(F(recursive[T, F])).flatMap(_.equal(T.project(x), T.project(y))))
}
