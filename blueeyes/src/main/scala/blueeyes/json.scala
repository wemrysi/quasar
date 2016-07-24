package blueeyes

import scalaz._, Scalaz._
import blueeyes.json.serialization.Decomposer

package object json {
  type JField = (String, JValue)

  def jarray(elements: JValue*): JValue                                    = JArray(elements.toList)
  def jobject(fields: JField*): JValue                                     = JObject(fields.toList)
  def jfield[A](name: String, value: A)(implicit d: Decomposer[A]): JField = JField(name, d(value))

  private def ppath(p: String) = if (p startsWith ".") p else "." + p

  implicit def liftJPathField(name: String): JPathNode = JPathField(name)
  implicit def liftJPathIndex(index: Int): JPathNode   = JPathIndex(index)
  implicit def liftJPath(path: String): JPath          = JPath(path)

  implicit val JPathNodeOrder: Order[JPathNode] = Order orderBy (x => x.optName -> x.optIndex)
  implicit val JPathNodeOrdering                = JPathNodeOrder.toScalaOrdering
  implicit val JPathOrder: Order[JPath]         = Order orderBy (_.nodes)
  implicit val JPathOrdering                    = JPathOrder.toScalaOrdering

  implicit val JObjectMergeMonoid = new Monoid[JObject] {
    val zero = JObject(Nil)

    def append(v1: JObject, v2: => JObject): JObject = v1.merge(v2).asInstanceOf[JObject]
  }

  val NoJPath     = JPath()
  type JPath      = quasar.precog.JPath
  type JPathNode  = quasar.precog.JPathNode
  type JPathField = quasar.precog.JPathField
  val JPathField  = quasar.precog.JPathField
  type JPathIndex = quasar.precog.JPathIndex
  val JPathIndex  = quasar.precog.JPathIndex

  private[json] def buildString(f: StringBuilder => Unit): String = {
    val sb = new StringBuilder
    f(sb)
    sb.toString
  }

  implicit class JPathOps(private val x: JPath) {
    import x._

    def parent: Option[JPath]        = if (nodes.isEmpty) None else Some(JPath(nodes dropRight 1: _*))
    def apply(index: Int): JPathNode = nodes(index)
    def head: Option[JPathNode]      = nodes.headOption
    def tail: JPath                  = JPath(nodes.tail)
    def path: String                 = x.to_s

    def ancestors: List[JPath] = {
      def loop(path: JPath, acc: List[JPath]): List[JPath] = path.parent.fold(acc)(p => loop(p, p :: acc))
      loop(x, Nil).reverse
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
  }

  implicit class JPathNodeOps(private val x: JPathNode) {
    import x._
    def optName: Option[String] = x match {
      case JPathField(x) => Some(x)
      case _             => None
    }
    def optIndex: Option[Int] = x match {
      case JPathIndex(x) => Some(x)
      case _             => None
    }
    def \(that: JPath)     = JPath(x :: that.nodes)
    def \(that: JPathNode) = JPath(x :: that :: Nil)
  }
}

package json {
  object JPath {
    def apply(path: String): JPath = {
      val PathPattern  = """[.]|(?=\[\d+\])""".r
      val IndexPattern = """^\[(\d+)\]$""".r
      def ppath(p: String) = if (p startsWith ".") p else "." + p
      JPath(
        PathPattern split ppath(path) map (_.trim) flatMap {
          case ""                  => None
          case IndexPattern(index) => Some(JPathIndex(index.toInt))
          case name                => Some(JPathField(name))
        } toList
      )
    }
    def apply(n: JPathNode*): JPath                 = new JPath(n.toList)
    def apply(n: List[JPathNode]): JPath            = new JPath(n)
    def unapply(path: JPath): Some[List[JPathNode]] = Some(path.nodes)
  }
}
