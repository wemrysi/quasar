/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.Predef._

import scalaz._
import simulacrum.typeclass

@typeclass trait ElideBuckets[F[_]] {
  type IT[G[_]]

  type InnerPure = IT[QScriptPure[IT, ?]]

  def purify: F[InnerPure] => QScriptPure[IT, InnerPure]
}

object ElideBuckets extends Elide2 {
  type Aux[T[_[_]], F[_]] = ElideBuckets[F] { type IT[G[_]] = T[G] }
}

trait Elide1 {
  implicit def generalElideBuckets[T[_[_]], G[_]](
    implicit G: G :<: QScriptPure[T, ?]):
      ElideBuckets.Aux[T, G] =
    new ElideBuckets[G] {
      type IT[K[_]] = T[K]

      def purify: G[InnerPure] => QScriptPure[IT, InnerPure] = G.inj
    }
}

trait Elide2 extends Elide1 {
  implicit def coproductElideBuckets[T[_[_]], F[_], G[_]](
    implicit mf: ElideBuckets.Aux[T, F],
             mg: ElideBuckets.Aux[T, G]):
      ElideBuckets.Aux[T, Coproduct[F, G, ?]] =
    new ElideBuckets[Coproduct[F, G, ?]] {
      type IT[K[_]] = T[K]
  
      def purify: Coproduct[F, G, InnerPure] => QScriptPure[IT, InnerPure] = _.run match {
        case -\/(f) => mf.purify(f)
        case \/-(g) => mg.purify(g)
      }
    }
}
