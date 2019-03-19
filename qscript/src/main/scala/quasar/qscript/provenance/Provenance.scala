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

trait Provenance[S, V, T] {
  type P

  def autojoinKeys(l: P, r: P): JoinKeys[V]

  /** The conjunction of two provenance. */
  def and(l: P, r: P): P

  /** Provenance having a dimensionality of zero. */
  def empty: P

  /** Append an identity, increasing dimensionality by 1. */
  def inflate(vectorId: V, sort: T, p: P): P

  /** Append an identity, maintaining current dimensionality. */
  def inflateConjoin(vectorId: V, sort: T, p: P): P

  /** "Submerge" an identity, making it the second-highest dimension,
    * increasing dimensionality by 1.
    */
  def inflateSubmerge(vectorId: V, sort: T, p: P): P

  /** Inject into a structure at an unknown field. */
  def injectDynamic(p: P): P

  /** Inject into a structure at the given field. */
  def injectStatic(scalarId: S, sort: T, p: P): P

  /** The disjunction of two provenance. */
  def or(l: P, r: P): P

  /** Project an unknown field. */
  def projectDynamic(p: P): P

  /** Project a statically known field. */
  def projectStatic(scalarId: S, sort: T, p: P): P

  /** Discard the highest dimension. */
  def reduce(p: P): P

  // Optics

  def scalarIds: Traversal[P, (S, T)]

  def vectorIds: Traversal[P, (V, T)]
}

object Provenance {
  type Aux[S, V, T, P0] = Provenance[S, V, T] { type P = P0 }
}
