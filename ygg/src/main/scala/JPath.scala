package ygg.json

import ygg.common._

final case class JPath(nodes: Vec[JPathNode]) extends ToString {
  def to_s: String = nodes match {
    case Seq() => "."
    case _     => nodes mkString ""
  }
}

sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String)         extends JPathNode("." + name)
final case class JPathIndex(index: Int)           extends JPathNode(s"[$index]")

object JPath {
  private val PathPattern  = """[.]|(?=\[\d+\])""".r
  private val IndexPattern = """^\[(\d+)\]$""".r
  private def ppath(p: String) = if (p startsWith ".") p else "." + p

  def apply(xs: List[JPathNode]): JPath = new JPath(xs.toVector)
  def apply(n: JPathNode*): JPath = new JPath(n.toVector)
  def apply(path: String): JPath = JPath(
    PathPattern split ppath(path) map (_.trim) flatMap {
      case ""                  => None
      case IndexPattern(index) => Some(JPathIndex(index.toInt))
      case name                => Some(JPathField(name))
    } toVector
  )
}
