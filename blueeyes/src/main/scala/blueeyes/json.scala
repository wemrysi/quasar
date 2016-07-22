package blueeyes

import json.serialization.Decomposer
import scalaz._, Scalaz._

package object json {
  type JField = (String, JValue)

  def jarray(elements: JValue*): JValue                                    = JArray(elements.toList)
  def jobject(fields: JField*): JValue                                     = JObject(fields.toList)
  def jfield[A](name: String, value: A)(implicit d: Decomposer[A]): JField = JField(name, d(value))

  implicit def liftJPathField(name: String): JPathNode = JPathField(name)
  implicit def liftJPathIndex(index: Int): JPathNode   = JPathIndex(index)
  implicit def liftJPath(path: String): JPath = {
    val PathPattern  = """\.|(?=\[\d+\])""".r
    val IndexPattern = """^\[(\d+)\]$""".r

    def parse0(segments: List[String], acc: List[JPathNode]): List[JPathNode] = segments match {
      case Nil                         => acc.reverse
      case hd :: tl if hd.trim.isEmpty => parse0(tl, acc)
      case IndexPattern(index) :: tl   => parse0(tl, JPathIndex(index.toInt) :: acc)
      case name :: tl                  => parse0(tl, JPathField(name) :: acc)
    }
    val properPath = if (path.startsWith(".")) path else "." + path

    JPath(parse0(PathPattern.split(properPath).toList, Nil))
  }

  val MergeMonoid = new Monoid[JValue] {
    val zero = JUndefined

    def append(v1: JValue, v2: => JValue): JValue = v1.merge(v2)
  }

  val ConcatMonoid = new Monoid[JValue] {
    val zero = JUndefined

    def append(v1: JValue, v2: => JValue): JValue = v1 ++ v2
  }
  implicit val JPathNodeOrder: Order[JPathNode] = Order orderBy (x => x.optName -> x.optIndex)
  implicit val JPathNodeOrdering                = JPathNodeOrder.toScalaOrdering
  implicit val JPathOrder: Order[JPath]         = Order orderBy (_.nodes)
  implicit val JPathOrdering                    = JPathOrder.toScalaOrdering

  implicit val JObjectMergeMonoid = new Monoid[JObject] {
    val zero = JObject(Nil)

    def append(v1: JObject, v2: => JObject): JObject = v1.merge(v2).asInstanceOf[JObject]
  }

  private[json] def buildString(f: StringBuilder => Unit): String = {
    val sb = new StringBuilder
    f(sb)
    sb.toString
  }
}
