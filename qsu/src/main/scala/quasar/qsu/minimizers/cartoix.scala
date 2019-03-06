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

import quasar.IdStatus
import quasar.common.CPathNode
import quasar.qscript.{FreeMap, FreeMapA}

import scala.{Int, List, Product, Serializable, Symbol}
import scala.collection.{Map => SMap}

private[minimizers] final case class Cartouche[T[_[_]]](
    stages: List[CStage[T]],
    dims: QDims[T])

private[minimizers] sealed trait CStage[T[_[_]]] extends Product with Serializable {
  val incoming: FreeMap[T]
}

private[minimizers] object CStage {
  import QScriptUniform.Rotation

  /**
   * Represents a divergence in the graph, as if millions of rows
   * sudden cried out in terror and were suddenly silenced.
   */
  final case class Join[T[_[_]]](
      incoming: FreeMap[T],
      cartoix: SMap[Symbol, Cartouche[T]],
      joiner: FreeMapA[T, CartoucheRef])
      extends CStage[T]

  final case class Shift[T[_[_]]](
      incoming: FreeMap[T],
      idStatus: IdStatus,
      rot: Rotation)
      extends CStage[T]

  final case class Project[T[_[_]]](
      incoming: FreeMap[T],
      node: CPathNode)
      extends CStage[T]
}

private[minimizers] sealed trait CartoucheRef extends Product with Serializable {
  val ref: Symbol
}

private[minimizers] object CartoucheRef {

  final case class Final(ref: Symbol) extends CartoucheRef

  // offset from top of cartouche (head of list) which contains an IncludeId
  final case class Offset(ref: Symbol, offset: Int) extends CartoucheRef
}
