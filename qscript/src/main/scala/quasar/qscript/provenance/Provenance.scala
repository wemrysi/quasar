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

package quasar.qscript.provenance

import monocle.Traversal

import cats.kernel.{BoundedSemilattice, CommutativeMonoid}

import scalaz.@@
import scalaz.Tags.{Disjunction, Conjunction}
import scalaz.syntax.tag._

trait Provenance[S, V, T] { self =>
  type P

  /** The set of identity comparisons describing an autojoin of `l` and `r`. */
  def autojoin(l: P, r: P): JoinKeys[S, V]

  /** The conjunction of two provenance, representing a dataset where values
    * have identities from both inputs.
    */
  def and(l: P, r: P): P

  /** Provenance having a dimensionality of zero. */
  def empty: P

  /** Append an identity, maintaining current dimensionality. */
  def inflateConjoin(vectorId: V, sort: T, p: P): P

  /** Append an identity, increasing dimensionality by 1. */
  def inflateExtend(vectorId: V, sort: T, p: P): P

  /** "Submerge" an identity, making it the second-highest dimension,
    * increasing dimensionality by 1.
    */
  def inflateSubmerge(vectorId: V, sort: T, p: P): P

  /** Inject into a structure at an unknown field, maintains dimensionality. */
  def injectDynamic(p: P): P

  /** Inject into a structure at the given field, maintains dimensionality. */
  def injectStatic(scalarId: S, sort: T, p: P): P

  /** The disjunction of two provenance, representing a dataset where values
    * have identities from either of the inputs.
    */
  def or(l: P, r: P): P

  /** Project an unknown field, maintains dimensionality. */
  def projectDynamic(p: P): P

  /** Project a statically known field, maintains dimensionality. */
  def projectStatic(scalarId: S, sort: T, p: P): P

  /** Discard the highest dimension, reducing dimensionality by 1. */
  def reduce(p: P): P

  // Optics

  def scalarIds: Traversal[P, (S, T)]

  def vectorIds: Traversal[P, (V, T)]

  object instances {
    implicit val pConjunctionCommutativeMonoid: CommutativeMonoid[P @@ Conjunction] =
      new CommutativeMonoid[P @@ Conjunction] {
        val empty = Conjunction(self.empty)

        def combine(x: P @@ Conjunction, y: P @@ Conjunction) =
          Conjunction(and(x.unwrap, y.unwrap))
      }

    implicit val pDisjunctionSemilattice: BoundedSemilattice[P @@ Disjunction] =
      new BoundedSemilattice[P @@ Disjunction] {
        val empty = Disjunction(self.empty)

        def combine(x: P @@ Disjunction, y: P @@ Disjunction) =
          Disjunction(or(x.unwrap, y.unwrap))
      }
  }

  object syntax {
    implicit final class ProvenanceOps(p: P) {
      def ⋈ (that: P): JoinKeys[S, V] =
        self.autojoin(p, that)

      def ∧ (that: P): P =
        self.and(p, that)

      def ∨ (that: P): P =
        self.or(p, that)

      def inflateConjoin(vectorId: V, sort: T): P =
        self.inflateConjoin(vectorId, sort, p)

      def inflateExtend(vectorId: V, sort: T): P =
        self.inflateExtend(vectorId, sort, p)

      def inflateSubmerge(vectorId: V, sort: T): P =
        self.inflateSubmerge(vectorId, sort, p)

      def injectDynamic: P =
        self.injectDynamic(p)

      def injectStatic(scalarId: S, sort: T): P =
        self.injectStatic(scalarId, sort, p)

      def projectDynamic: P =
        self.projectDynamic(p)

      def projectStatic(scalarId: S, sort: T): P =
        self.projectStatic(scalarId, sort, p)

      def reduce: P =
        self.reduce(p)
    }
  }
}

object Provenance {
  type Aux[S, V, T, P0] = Provenance[S, V, T] { type P = P0 }
}
