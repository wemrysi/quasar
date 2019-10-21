/*
 * Copyright 2014–2019 SlamData Inc.
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

import quasar.{IdStatus, NonTerminal, RenderTree, RenderTreeT, Terminal}
import quasar.RenderTree.ops._
import quasar.contrib.iota._
import quasar.fp._
import quasar.qscript.{FreeMap, FreeMapA}

import matryoshka._
import matryoshka.data.free._

import scala.collection.immutable.{Map => SMap}

import scalaz.{Equal, Show}
import scalaz.Scalaz._

/**
 * @tparam P path
 * @tparam S struct
 */
private[minimizers] sealed trait CStage[T[_[_]], +P, +S] extends Product with Serializable

private[minimizers] object CStage {
  import QScriptUniform.Rotation

  /**
   * Represents a divergence in the graph, as if millions of rows
   * cried out in terror and were suddenly silenced.
   */
  final case class Join[T[_[_]], P, S](
      cartoix: SMap[Symbol, Cartouche[T, P, S]],
      joiner: FreeMapA[T, CartoucheRef])
      extends CStage[T, P, S]

  /** Represents a cartesian between the cartoix, producing a map containing
    * the result of each cartouche at the cooresponding symbol.
    *
    * Used to represent intermediate results when simplifying joins.
    */
  final case class Cartesian[T[_[_]], P, S](
      cartoix: SMap[Symbol, Cartouche[T, P, S]])
      extends CStage[T, P, S]

  final case class Shift[T[_[_]], S](
      struct: S,
      idStatus: IdStatus,
      rot: Rotation)
      extends CStage[T, Nothing, S]

  final case class Project[T[_[_]], P](path: P)
      extends CStage[T, P, Nothing]

  final case class Expr[T[_[_]]](f: FreeMap[T])
      extends CStage[T, Nothing, Nothing]

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT, P: Show, S: RenderTree]: RenderTree[CStage[T, P, S]] =
    RenderTree make {
      case Join(cart, join) =>
        NonTerminal(List("Join"), None,
          cart.render.children :+
          NonTerminal(List("Joiner"), None, List(join.render)))

      case Cartesian(cart) =>
        NonTerminal(List("Cartesian"), None, cart.render.children)

      case Shift(struct, status, rot) =>
        NonTerminal(List("Shift"), Some(s"status = $status, rotation = $rot"), List(
          NonTerminal(List("Struct"), None, List(struct.render))))

      case Project(path) =>
        Terminal(List("Project"), Some(path.shows))

      case Expr(f) =>
        NonTerminal(List("Expr"), None, List(f.render))
    }

  implicit def equal[T[_[_]]: BirecursiveT: EqualT, P: Equal, S: Equal]: Equal[CStage[T, P, S]] =
    Equal equal {
      case (Join(c1, j1), Join(c2, j2)) => c1 === c2 && j1 === j2
      case (Cartesian(c1), Cartesian(c2)) => c1 === c2
      case (Shift(st1, id1, rot1), Shift(st2, id2, rot2)) => st1 === st2 && id1 === id2 && rot1 === rot2
      case (Project(l), Project(r)) => l === r
      case (Expr(l), Expr(r)) => l === r
      case _ => false
    }
}
