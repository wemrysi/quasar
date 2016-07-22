package blueeyes
package json

import serialization._
import serialization.DefaultSerialization._
import scalaz._, Scalaz._, Ordering._

final class JPath(val nodes: List[JPathNode]) {
  def parent: Option[JPath]        = if (nodes.isEmpty) None else Some(JPath(nodes.take(nodes.length - 1): _*))
  def apply(index: Int): JPathNode = nodes(index)
  def head: Option[JPathNode]      = nodes.headOption
  def tail: JPath                  = JPath(nodes.tail)
  def path: String                 = nodes.map(_.render).mkString("")

  def ancestors: List[JPath] = {
    def loop(path: JPath, acc: List[JPath]): List[JPath] = path.parent.fold(acc)(p => loop(p, p :: acc))
    loop(this, Nil).reverse
  }

  def \(that: JPath): JPath   = JPath(nodes ++ that.nodes)
  def \(that: String): JPath  = JPath(nodes :+ JPathField(that))
  def \(that: Int): JPath     = JPath(nodes :+ JPathIndex(that))
  def \:(that: JPath): JPath  = JPath(that.nodes ++ nodes)
  def \:(that: String): JPath = JPath(JPathField(that) +: nodes)
  def \:(that: Int): JPath    = JPath(JPathIndex(that) +: nodes)

  def dropPrefix(p: JPath): Option[JPath] = {
    def remainder(nodes: List[JPathNode], toDrop: List[JPathNode]): Option[JPath] = {
      nodes match {
        case x :: xs =>
          toDrop match {
            case `x` :: ys => remainder(xs, ys)
            case Nil       => Some(JPath(nodes))
            case _         => None
          }

        case Nil =>
          if (toDrop.isEmpty) Some(JPath(nodes))
          else None
      }
    }

    remainder(nodes, p.nodes)
  }
  def extract(jvalue: JValue): JValue = {
    def extract0(path: List[JPathNode], d: JValue): JValue = path match {
      case Nil                     => d
      case JPathField(name) :: tl  => extract0(tl, d \ name)
      case JPathIndex(index) :: tl => extract0(tl, d(index))
    }
    extract0(nodes, jvalue)
  }
  def expand(jvalue: JValue): List[JPath] = {
    def expand0(current: List[JPathNode], right: List[JPathNode], d: JValue): List[JPath] = right match {
      case Nil                            => JPath(current) :: Nil
      case (hd @ JPathIndex(index)) :: tl => expand0(current :+ hd, tl, jvalue(index))
      case (hd @ JPathField(name)) :: tl  => expand0(current :+ hd, tl, jvalue \ name)
    }
    expand0(Nil, nodes, jvalue)
  }

  override def toString = if (nodes.isEmpty) "." else path
}

sealed trait JPathNode {
  def optName: Option[String] = this match {
    case JPathField(x) => Some(x)
    case _             => None
  }
  def optIndex: Option[Int] = this match {
    case JPathIndex(x) => Some(x)
    case _             => None
  }
  def \(that: JPath)     = JPath(this :: that.nodes)
  def \(that: JPathNode) = JPath(this :: that :: Nil)
  def render: String
}
sealed case class JPathField(name: String) extends JPathNode {
  def render            = s".$name"
  override def toString = render //FIXME
}

sealed case class JPathIndex(index: Int) extends JPathNode {
  def render            = s"[$index]"
  override def toString = render
}

object JPath {
  val Identity = apply()

  def apply(n: JPathNode*): JPath                    = apply(n.toList)
  def apply(l: List[JPathNode]): JPath               = new JPath(l)
  def unapplySeq(path: JPath): Some[List[JPathNode]] = Some(path.nodes)
}
