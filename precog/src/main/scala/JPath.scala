package quasar
package precog

final case class JPath(nodes: List[JPathNode]) extends ToString {
  def to_s: String = nodes match {
    case Nil => "."
    case _   => nodes mkString ""
  }
}

sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String) extends JPathNode("." + name)
final case class JPathIndex(index: Int) extends JPathNode(s"[$index]")
