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

package quasar.contrib.matryoshka

import matryoshka._
import matryoshka.implicits._
import scalaz._
import simulacrum._

@typeclass
trait OrderT[T[_[_]]] {
  def order[F[_]: Functor](tf1: T[F], tf2: T[F])(implicit del: Delay[Order, F]): Ordering

  def orderT[F[_]: Functor](delay: Delay[Order, F]): Order[T[F]] =
    Order.order[T[F]](order[F](_, _)(Functor[F], delay))
}

object OrderT {
  implicit def recursiveT[T[_[_]]: RecursiveT]: OrderT[T] =
    new OrderT[T] {
      def order[F[_]: Functor](tf1: T[F], tf2: T[F])(implicit del: Delay[Order, F]): Ordering =
        del(orderT[F](del)).order(tf1.project, tf2.project)
    }
}
