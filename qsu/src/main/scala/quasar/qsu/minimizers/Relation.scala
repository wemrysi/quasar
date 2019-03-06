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

package quasar.qsu
package minimizers

import slamdata.Predef._

import scala.collection.mutable

private[minimizers] final case class Relation(index: Map[Symbol, Set[Symbol]]) {
  private[this] val memo = mutable.Map[Symbol, Set[Symbol]]

  /**
   * Produces the reflexive transitive closure of the directed
   * relation represented by `index`.
   */
  def closure(ref: Symbol): Set[Symbol] = {
    def loop(ref: Symbol): Set[Symbol] = {
      if (memo.contains(ref)) {
        memo(ref)
      } else {
        index.get(ref) match {
          case Some(refs) if !refs.isEmpty =>
            val results = refs.flatMap(loop)
            memo += (ref -> results)
            results

          case _ =>
            memo += (ref -> Set())
            Set()
        }
      }
    }

    loop(ref)
  }

  def +(pair: (Symbol, Symbol)): Relation = {
    val (left, right) = pair

    val to = index.getOrElse(left, Set())
    val from = index.getOrElse(right, Set())

    Relation(index + (left -> (to + right)) + (right -> (from + left)))
  }

  def closures: Set[Set[Symbol]] = {
    def loop(remaining: Set[Symbol]): Set[Set[Symbol]] = {
      val sym = remaining.head    // axiom of choice ftw
      val cl = closure(sym)
      loop(remaining -- cl) + cl
    }

    loop(index.keys.toSet)
  }
}

private[minimizers] object Relation extends (Map[Symbol, Set[Symbol]] => Relation) {

  val Empty: Relation = Relation(Map())

  def allPairs[F[_]: Foldable, A](fa: F[A])(p: (A, A) => Boolean): Relation = {
    def loop(pairs: List[A], acc: Relation): Relation = pairs match {
      case hd :: tail =>
        tail.filter(p(hd, _)).foldLeft(acc)((acc, a) => acc + (hd -> a))

      case Nil => acc
    }

    loop(fa.toList, Empty)
  }
}
