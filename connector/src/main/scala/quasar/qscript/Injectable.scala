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

import scalaz._, Scalaz._

/** This is like [[scalaz.Inject]], but for injecting an arbitrary coproduct
  * where all of the components are in the `OUT` coproduct in any order.
  *
  * It _may_ be unprincipled (otherwise, why not allow [[scalaz.Inject]] to work
  * this way directly?) But it is temporarily necessary in order to “inject” our
  * more constrained versions of QScript into [[QScriptTotal]].
  */
trait Injectable[IN[_]] {
  type OUT[A]
  def inject: IN ~> OUT
  def project[A]: OUT[A] => Option[IN[A]]
}

object Injectable extends Injectable0 {
  type Aux[IN[_], F[_]] = Injectable[IN] { type OUT[A] = F[A] }

  implicit def coproduct[F[_], G[_], H[_]]
    (implicit F: Injectable.Aux[F, H], G: Injectable.Aux[G, H])
      : Injectable.Aux[Coproduct[F, G, ?], H] =
    new Injectable[Coproduct[F, G, ?]] {
      type OUT[A] = H[A]
      def inject = new (Coproduct[F, G, ?] ~> OUT) {
        def apply[A](fa: Coproduct[F, G, A]) =
          fa.run.fold(F.inject, G.inject)
      }

      def project[A] =
        out => F.project[A](out).fold[Option[Coproduct[F, G, A]]](
          G.project[A](out) ∘ (g => Coproduct(\/-(g))))(
          f => Coproduct(-\/(f)).some)
    }
}

abstract class Injectable0 {
  implicit def inject[IN[_], F[_]](implicit IN: IN :<: F)
      : Injectable.Aux[IN, F] =
    new Injectable[IN] {
      type OUT[A] = F[A]

      def inject = IN

      def project[A] = IN.prj[A]
    }
}
