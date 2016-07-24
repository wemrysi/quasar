package quasar
package precog

final class JPath(val nodes: List[JPathNode]) extends ToString {
  override def hashCode = nodes.##
  override def equals(x: Any) = x match {
    case x: JPath => nodes == x.nodes
    case _        => false
  }
  def to_s: String = nodes match {
    case Nil => "."
    case _   => nodes mkString ""
  }
}
sealed abstract class JPathNode(val to_s: String) extends ToString
final case class JPathField(name: String) extends JPathNode("." + name)
final case class JPathIndex(index: Int) extends JPathNode(s"[$index]")

// object JPath {
//   private val PathPattern  = """[.]|(?=\[\d+\])""".r
//   private val IndexPattern = """^\[(\d+)\]$""".r

//   val Identity = new JPath(Nil)

//   private def ppath(p: String) = if (p startsWith ".") p else "." + p
//   def apply(path: String): JPath = apply(
//     PathPattern split ppath(path) map (_.trim) flatMap {
//       case ""                  => None
//       case IndexPattern(index) => Some(JPathIndex(index.toInt))
//       case name                => Some(JPathField(name))
//     } toList
//   )
//   def apply(n: JPathNode*): JPath                    = new JPath(n.toList)
//   def apply(n: List[JPathNode]): JPath               = new JPath(n)

//   def unapply(path: JPath): Some[List[JPathNode]] = Some(path.nodes)
//   // def unapplySeq(path: JPath): Some[List[JPathNode]] = Some(path.nodes)
// }
