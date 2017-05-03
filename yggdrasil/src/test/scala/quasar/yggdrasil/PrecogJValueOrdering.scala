package quasar.yggdrasil

import quasar.blueeyes._, json._
import scalaz._, Scalaz._

/**
 * This provides an ordering on JValue that mimics how we'd order them as
 * columns in a table, rather than using JValue's default ordering which
 * behaves differently.
 */
trait PrecogJValueOrder extends ScalazOrder[JValue] {
  def order(a: JValue, b: JValue): Ordering = {
    val prims0 = a.flattenWithPath.toMap
    val prims1 = b.flattenWithPath.toMap
    val cols0  = (prims1.mapValues { _ => JUndefined } ++ prims0).toList.sortMe
    val cols1  = (prims0.mapValues { _ => JUndefined } ++ prims1).toList.sortMe

    ScalazOrder[Vector[JPath -> JValue]].order(cols0, cols1)
  }
}

object PrecogJValueOrder {
  implicit object order extends PrecogJValueOrder
  implicit def ordering: ScalaMathOrdering[JValue] = order.toScalaOrdering
}
