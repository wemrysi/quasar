package quasar
package precog

final case class JPath(nodes: List[JPathNode]) extends ToString {
  def to_s: String = nodes match {
    case Nil => "."
    case _   => nodes mkString ""
  }
}

sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String)         extends JPathNode("." + name)
final case class JPathIndex(index: Int)           extends JPathNode(s"[$index]")

object JPathNode {
  implicit def liftString(s: String): JPathNode = JPathField(s)
  implicit def liftIndex(i: Int): JPathNode     = JPathIndex(i)
}
