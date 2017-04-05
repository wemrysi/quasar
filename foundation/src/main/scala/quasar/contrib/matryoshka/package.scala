/*
 * Copyright 2014–2017 SlamData Inc.
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

import _root_.matryoshka._
import _root_.scalaz._, Scalaz._

package object matryoshka {
  implicit def delayOrder[F[_], A](implicit F: Delay[Order, F], A: Order[A]): Order[F[A]] =
    F(A)

  implicit def orderTOrder[T[_[_]], F[_]: Functor](implicit T: OrderT[T], F: Delay[Order, F]): Order[T[F]] =
    T.orderT[F](F)

  implicit def coproductOrder[F[_], G[_]](implicit F: Delay[Order, F], G: Delay[Order, G]): Delay[Order, Coproduct[F, G, ?]] =
    new Delay[Order, Coproduct[F, G, ?]] {
      def apply[A](ord: Order[A]): Order[Coproduct[F, G, A]] = {
        implicit val ordA: Order[A] = ord
        Order.orderBy((_: Coproduct[F, G, A]).run)
      }
    }

  /** Chains multiple transformations together, each of which can fail to change
    * anything.
    */
  def applyTransforms[A](first: A => Option[A], rest: (A => Option[A])*)
      : A => Option[A] =
    rest.foldLeft(
      first)(
      (prev, next) => x => prev(x).fold(next(x))(orOriginal(next)(_).some))
}
