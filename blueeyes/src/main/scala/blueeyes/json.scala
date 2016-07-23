package blueeyes

import scalaz._, Scalaz._
import blueeyes.json.serialization.Decomposer

package object json {
  type JField = (String, JValue)

  def jarray(elements: JValue*): JValue                                    = JArray(elements.toList)
  def jobject(fields: JField*): JValue                                     = JObject(fields.toList)
  def jfield[A](name: String, value: A)(implicit d: Decomposer[A]): JField = JField(name, d(value))

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

  private[json] def buildString(f: StringBuilder => Unit): String = {
    val sb = new StringBuilder
    f(sb)
    sb.toString
  }
}
