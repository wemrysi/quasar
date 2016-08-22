package ygg.table

import ygg.common._
import scalaz._, Scalaz._, Ordering._

sealed trait CPathNode {
  def \(that: CPath): CPath     = CPath(this :: that.nodes)
  def \(that: CPathNode): CPath = CPath(this :: that :: Nil)
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
