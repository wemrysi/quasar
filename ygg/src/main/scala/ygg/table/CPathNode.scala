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

package ygg.table

import ygg.common._
import scalaz._, Scalaz._, Ordering._

sealed trait CPathNode extends Product with Serializable {
  def \(that: CPath): CPath     = CPath(this +: that.nodes)
  def \(that: CPathNode): CPath = CPath(Vec(this, that))
  final override def toString = this match {
    case CPathField(name)  => s".$name"
    case CPathMeta(name)   => s"@$name"
    case CPathIndex(index) => s"[$index]"
    case CPathArray        => "[*]"
  }
}
final case class CPathField(name: String) extends CPathNode
final case class CPathMeta(name: String) extends CPathNode
final case class CPathIndex(index: Int) extends CPathNode
final case object CPathArray extends CPathNode

object CPathNode {
  implicit def s2PathNode(name: String): CPathNode = CPathField(name)
  implicit def i2PathNode(index: Int): CPathNode   = CPathIndex(index)

  implicit object CPathNodeOrder extends Ord[CPathNode] {
    def order(n1: CPathNode, n2: CPathNode): Cmp = (n1, n2) match {
      case (CPathField(s1), CPathField(s2)) => Cmp(s1 compare s2)
      case (CPathField(_), _)               => GT
      case (_, CPathField(_))               => LT
      case (CPathArray, CPathArray)         => EQ
      case (CPathArray, _)                  => GT
      case (_, CPathArray)                  => LT
      case (CPathIndex(i1), CPathIndex(i2)) => i1 ?|? i2
      case (CPathIndex(_), _)               => GT
      case (_, CPathIndex(_))               => LT
      case (CPathMeta(m1), CPathMeta(m2))   => Cmp(m1 compare m2)
    }
  }

  implicit val CPathNodeOrdering = CPathNodeOrder.toScalaOrdering
}
