package ygg.json

import ygg.api.ToString

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

object JPath {
  def apply(n: JPathNode*): JPath = new JPath(n.toList)
  def apply(path: String): JPath = {
    val PathPattern      = """[.]|(?=\[\d+\])""".r
    val IndexPattern     = """^\[(\d+)\]$""".r
    def ppath(p: String) = if (p startsWith ".") p else "." + p
    JPath(
      PathPattern split ppath(path) map (_.trim) flatMap {
        case ""                  => None
        case IndexPattern(index) => Some(JPathIndex(index.toInt))
        case name                => Some(JPathField(name))
      } toList
    )
  }
}
