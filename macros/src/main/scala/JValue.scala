package ygg.json

import ast._
import scalaz._, Scalaz._, Ordering._
import scalaz.{ Order => Ord }
import scala.util.Try
import scala.util.Sorting.quickSort
import java.lang.Double.isInfinite
import scala.annotation.tailrec
import JValue._

/**
  * Data type for Json AST.
  */
sealed trait JValue {
  def typeIndex: Int = this match {
    case _: JUndefined.type => -1
    case _: JNull.type      => 0
    case _: JBool           => 1
    case _: JNum            => 4
    case _: JString         => 5
    case _: JArray          => 6
    case _: JObject         => 7
  }

  final override def toString: String = ast.CanonicalRenderer render this
  final override def hashCode: Int = this match {
    case JBool(x)   => x.##
    case JNum(x)    => x.##
    case JString(s) => s.##
    case JArray(x)  => x.##
    case JObject(x) => x.##
    case _          => System identityHashCode this
  }
  final override def equals(other: Any) = other match {
    case x: JValue => (this eq x) || (JValue.Order.order(this, x) == EQ)
    case _         => false
  }
}
sealed trait JBool extends JValue
sealed trait JNum extends JValue {
  def toBigDecimal: BigDecimal = this match {
    case JNumBigDec(x) => x
    case JNumStr(x)    => BigDecimal(x)
    case JNumLong(x)   => BigDecimal(x)
  }
}

final case object JUndefined                          extends JValue
final case object JNull                               extends JValue
final case class JString(value: String)               extends JValue
final case class JObject(fields: Map[String, JValue]) extends JValue
final case class JArray(elements: Vector[JValue])     extends JValue
final case object JTrue                               extends JBool
final case object JFalse                              extends JBool
final case class JNumLong(n: Long)                    extends JNum
final case class JNumStr(s: String)                   extends JNum
final case class JNumBigDec(b: BigDecimal)            extends JNum

object JValue {
  type JStringValue = String -> JValue

  implicit object Order extends Ord[JValue] {
    def order(x: JValue, y: JValue): Ordering = (x, y) match {
      case (JObject(m1), JObject(m2))     => fieldsCompare(m1, m2)
      case (JString(x), JString(y))       => x ?|? y
      case (JNumLong(x), JNumLong(y))     => x ?|? y
      case (JNumBigDec(x), JNumBigDec(y)) => x ?|? y
      case (JNum(x), JNum(y))             => x ?|? y
      case (JArray(x), JArray(y))         => x ?|? y
      case (JBool(x), JBool(y))           => x ?|? y
      case (x, y)                         => x.typeIndex ?|? y.typeIndex
    }
  }

  private def fieldsCompare(m1: Map[String, JValue], m2: Map[String, JValue]): Ordering = {
    @tailrec def rec(fields: Array[String], i: Int): Ordering = {
      if (i < fields.length) {
        val key = fields(i)
        val v1  = m1.getOrElse(key, JUndefined)
        val v2  = m2.getOrElse(key, JUndefined)
        if (v1 == JUndefined && v2 == JUndefined) rec(fields, i + 1)
        else if (v1 == JUndefined) GT
        else if (v2 == JUndefined) LT
        else {
          val cres = v1 ?|? v2
          if (cres == EQ) rec(fields, i + 1) else cres
        }
      } else EQ
    }
    val arr: Array[String] = (m1.keySet ++ m2.keySet).toArray
    quickSort(arr)
    rec(arr, 0)
  }
}
object JBool {
  def apply(value: Boolean): JBool         = if (value) JTrue else JFalse
  def unapply(value: JBool): Some[Boolean] = Some(value eq JTrue)
}
object JNum {
  def apply(value: Double): JValue             = if (value.isNaN || isInfinite(value)) JUndefined else apply(value.toString)
  def apply(value: String): JValue             = if (value == null) JUndefined else JNumStr(value)
  def apply(value: Int): JNum                  = JNumLong(value.toLong)
  def apply(value: Long): JNum                 = JNumLong(value)
  def apply(value: BigDecimal): JNum           = JNumBigDec(value)
  def unapply(value: JNum): Option[BigDecimal] = Try(value.toBigDecimal).toOption
}
final object JObject {
  val empty = JObject(Nil)

  object Fields {
    def unapply(m: JObject): Some[Vector[JField]] = Some(m.fields.toVector map (x => JField(x)))
  }

  def apply(fields: Traversable[JField]): JObject = JObject(fields.map(_.toTuple).toMap)
  def apply(fields: JField*): JObject             = JObject(fields.map(_.toTuple).toMap)
}
final object JArray extends (Vector[JValue] => JArray) {
  val empty = JArray(Vector.empty)

  def apply(vals: List[JValue]): JArray = JArray(vals.toVector)
  def apply(vals: JValue*): JArray      = JArray(vals.toVector)
}
final case class JField(name: String, value: JValue) extends Product2[String, JValue] {
  def _1                    = name
  def _2                    = value
  def toTuple: JStringValue = name -> value
  def isUndefined           = value == JUndefined
}
final object JField {
  def apply(x: JStringValue): JField = JField(x._1, x._2)

  implicit def liftTuple(x: JStringValue): JField = apply(x)

  def liftCollect(f: PartialFunction[JField, JField]): PartialFunction[JValue, JValue] = {
    case JObject.Fields(fields) if fields exists f.isDefinedAt => JObject(fields collect f)
  }
}
