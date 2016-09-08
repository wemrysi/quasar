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

package quasar.physical.mongodb.expression0 // HACK

import quasar.Predef._
import quasar._, Planner._
import quasar.fp.Inj
import quasar.physical.mongodb.Bson
import quasar.physical.mongodb.expression.DocVar // HACK
import quasar.jscore, jscore.JsFn

import matryoshka._
import scalaz._

trait ExprOpOps[EX[_]] {
  def simplify[F[_]](implicit I: Inj[EX, F]): AlgebraM[Option, EX, Fix[F]]

  def bson: Algebra[EX, Bson]

  /** "Literal" translation to JS. */
  def toJsSimple: AlgebraM[PlannerError \/ ?, EX, JsFn]

  def rewriteRefs0[F[_]: Functor](applyVar: PartialFunction[DocVar, DocVar])(implicit inj: Inj[EX, F]): AlgebraM[Option, EX, Fix[F]]

  // TODO: capture F in the typeclass?
  final def rewriteRefs[F[_]: Functor](applyVar: PartialFunction[DocVar, DocVar])(implicit inj: Inj[EX, F]): Algebra[EX, Fix[F]] = {
    val r0 = rewriteRefs0[F](applyVar)
    x => r0(x).getOrElse(Fix(inj(x)))
  }
}
object ExprOpOps {
  def apply[EX[_]](implicit ops: ExprOpOps[EX]) = ops

  implicit def coproduct[F[_], G[_]](implicit
      F: ExprOpOps[F],
      G: ExprOpOps[G])
      : ExprOpOps[Coproduct[F, G, ?]] =
    new ExprOpOps[Coproduct[F, G, ?]] {
      def injF[H[_]](implicit I: Inj[Coproduct[F, G, ?], H]): Inj[F, H] = I compose Inj[F, Coproduct[F, G, ?]]
      def injG[H[_]](implicit I: Inj[Coproduct[F, G, ?], H]): Inj[G, H] = I compose Inj[G, Coproduct[F, G, ?]]

      override def simplify[H[_]](implicit I: Inj[Coproduct[F, G, ?], H]) =
        _.run.fold(
          f => F.simplify(injF).apply(f),
          g => G.simplify(injG).apply(g))

      val bson: Algebra[Coproduct[F, G, ?], Bson] =
        _.run.fold(F.bson(_), G.bson(_))

      val toJsSimple: AlgebraM[PlannerError \/ ?, Coproduct[F, G, ?], JsFn] =
        _.run.fold(F.toJsSimple(_), G.toJsSimple(_))

      override def rewriteRefs0[H[_]: Functor](applyVar: PartialFunction[DocVar, DocVar])(implicit inj: Inj[Coproduct[F, G, ?], H]) = {
        val rf = F.rewriteRefs0[H](applyVar)(Functor[H], injF)
        val rg = G.rewriteRefs0[H](applyVar)(Functor[H], injG)
        _.run.fold(rf, rg)
      }
    }
}

