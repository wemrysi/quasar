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

package quasar.impl.provenance

import slamdata.Predef.{None, Option, Some}

import quasar.qscript.provenance.{JoinKey, JoinKeys, Provenance, Uop}

import cats.Order
import cats.instances.list._
import cats.syntax.eq._
import cats.syntax.foldable._

import monocle.Traversal

import scalaz.@@
import scalaz.Tags.{Conjunction, Disjunction}
import scalaz.syntax.tag._

trait ProvImpl[S, V, T] extends Provenance[S, V, T] {

  implicit def sOrder: Order[S]
  implicit def vOrder: Order[V]
  implicit def tOrder: Order[T]

  private val D = Dim.Optics[S, V, T]

  type P = Uop[Identities[Dim[S, V, T]]]

  def autojoin(l: P, r: P): JoinKeys[S, V] = {
    import JoinKeys._

    def join(x: Dim[S, V, T], y: Dim[S, V, T]): Option[JoinKeys[S, V] @@ Conjunction] =
      Conjunction.subst((x, y) match {
        case (D.inflate(xv, xt), D.inflate(yv, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.dynamic(xv, yv)))

        case (D.inflate(xv, xt), D.project(ys, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.staticR(xv, ys)))

        case (D.project(xs, xt), D.inflate(yv, yt)) if xt === yt =>
          Some(JoinKeys.one(JoinKey.staticL(xs, yv)))

        case (xp @ D.project(_, _), yp @ D.project(_, _)) if xp === yp =>
          Some(JoinKeys.empty)

        case _ => None
      })

    val joins = for {
      lids <- l.toList
      rids <- r.toList
    } yield lids.zipWithDefined(rids)(join)

    joins.foldMap(jks => Disjunction(jks.unwrap)).unwrap
  }

  def and(l: P, r: P): P =
    l ∧ r

  def empty: P =
    Uop.empty

  def inflateConjoin(vectorId: V, sort: T, p: P): P =
    p.map(_ :≻ D.inflate(vectorId, sort))

  def inflateExtend(vectorId: V, sort: T, p: P): P =
    createOrSnoc(D.inflate(vectorId, sort), p)

  def inflateSubmerge(vectorId: V, sort: T, p: P): P =
    p.map(_.submerge(D.inflate(vectorId, sort)))

  def injectDynamic(p: P): P =
    p.map(_ :≻ D.fresh())

  def injectStatic(scalarId: S, sort: T, p: P): P =
    p.map(_ :≻ D.inject(scalarId, sort))

  def or(l: P, r: P): P =
    l ∨ r

  def projectDynamic(p: P): P =
    createOrConj(D.fresh(), p)

  def projectStatic(scalarId: S, sort: T, p: P): P =
    createOrConj(D.project(scalarId, sort), p)

  def reduce(p: P): P =
    Uop.of(p.toList.flatMap(_.init.toList): _*)

  // Optics

  val scalarIds: Traversal[P, (S, T)] =
    UopT composeTraversal Identities.values composeTraversal D.scalar

  val vectorIds: Traversal[P, (V, T)] =
    UopT composeTraversal Identities.values composePrism D.inflate

  ////

  private def UopT =
    Uop.values[Identities[Dim[S, V, T]], Identities[Dim[S, V, T]]]

  private def createOrConj(d: Dim[S, V, T], p: P): P =
    if (p.isEmpty)
      Uop.of(Identities(d))
    else
      p.map(_ :≻ d)

  private def createOrSnoc(d: Dim[S, V, T], p: P): P =
    if (p.isEmpty)
      Uop.of(Identities(d))
    else
      p.map(_ :+ d)
}

object ProvImpl {
  def apply[S, V, T](implicit sord: Order[S], vord: Order[V], tord: Order[T])
      : Provenance.Aux[S, V, T, Uop[Identities[Dim[S, V, T]]]] =
    new ProvImpl[S, V, T] {
      val sOrder = sord
      val vOrder = vord
      val tOrder = tord
    }
}
