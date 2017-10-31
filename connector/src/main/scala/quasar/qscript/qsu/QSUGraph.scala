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

package quasar.qscript.qsu

import quasar.fp._
import slamdata.Predef.{Map => SMap, _}

import scalaz.syntax.equal._

final case class QSUGraph[T[_[_]]](
    root: Symbol,
    vertices: SMap[Symbol, QScriptUniform[T, Symbol]]) {

  def rename(from: Symbol, to: Symbol): QSUGraph[T] = {
    val root2 = if (from === root)
      from
    else
      root

    val vertices2 =
      vertices.get(from).map(node => vertices - from + (to -> node)).getOrElse(vertices)

    QSUGraph(root2, vertices2)
  }

  /**
   * Uniquely merge the graphs, retaining the root from the right.
   */
  def ++:(left: QSUGraph[T]): QSUGraph[T] = QSUGraph(root, vertices ++ left.vertices)

  /**
   * Uniquely merge the graphs, retaining the root from the left.
   */
  def :++(right: QSUGraph[T]): QSUGraph[T] = right ++: this
}
