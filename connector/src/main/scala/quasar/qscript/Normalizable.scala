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

import matryoshka._
import scalaz._, Scalaz._
import simulacrum.typeclass

@typeclass trait Normalizable[F[_]] {
  def normalize: NTComp[F, Option]
}

// it would be nice to use the `NTComp` alias here, but it cannot compile
trait NormalizableInstances {
  implicit def const[A] = new Normalizable[Const[A, ?]] {
    def normalize = λ[Const[A, ?] ~> (Option ∘ Const[A, ?])#λ](_ => None)
  }
  implicit def coproduct[F[_]: Normalizable, G[_]: Normalizable] = new Normalizable[Coproduct[F, G, ?]] {
    def normalize = λ[Coproduct[F, G, ?] ~> (Option ∘ Coproduct[F, G, ?])#λ](
      _.run.bitraverse(Normalizable[F].normalize(_), Normalizable[G].normalize(_)).map(Coproduct(_)))
  }
}

object Normalizable extends NormalizableInstances {
  def make[F[_]](f: NTComp[F, Option]): Normalizable[F] = new Normalizable[F] { val normalize = f }
}
