/*
 * Copyright 2009-2010 WorldWide Conferencing, LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package blueeyes
package json

import scalaz._, Scalaz._, Ordering._, Validation._, FlatMap._
import scala.util.Sorting.quickSort

import java.lang.Double.isInfinite
import JValue.{ RenderMode, Compact, Pretty, Canonical }
import quasar.precog._

/**
  * Data type for Json AST.
  */
sealed trait JValue extends Merge.Mergeable with Diff.Diffable with Product with Ordered[JValue] with ToString { self =>
  protected[json] def typeIndex: Int
  def compare(that: JValue): Int
  def sort: JValue

  def to_s = this.renderPretty
}

object JValue {
  sealed trait RenderMode
  case object Pretty    extends RenderMode
  case object Compact   extends RenderMode
  case object Canonical extends RenderMode

  def apply(p: JPath, v: JValue) = JUndefined.set(p, v)

  private def unflattenArray(elements: Seq[(JPath, JValue)]): JArray = {
    elements.foldLeft(JArray(Nil)) { (arr, t) =>
      arr.set(t._1, t._2) --> classOf[JArray]
    }
  }

  private def unflattenObject(elements: Seq[(JPath, JValue)]): JObject = {
    elements.foldLeft(JObject(Nil)) { (obj, t) =>
      obj.set(t._1, t._2) --> classOf[JObject]
    }
  }

  def unflatten(elements: Seq[(JPath, JValue)]): JValue = {
    if (elements.isEmpty) JUndefined
    else {
      val sorted = elements.sortBy(_._1)

      val (xp, xv) = sorted.head

      if (xp == NoJPath && sorted.size == 1) xv
      else if (xp.path.startsWith("[")) unflattenArray(sorted)
      else unflattenObject(sorted)
    }
  }

  case class paired(jv1: JValue, jv2: JValue) {
    assert(jv1 != null && jv2 != null)
    final def fold[A](default: => A)(obj: Map[String, JValue] => Map[String, JValue] => A,
                                     arr: List[JValue] => List[JValue] => A,
                                     str: String => String => A,
                                     num: BigDecimal => BigDecimal => A,
                                     bool: Boolean => Boolean => A,
                                     nul: => A,
                                     nothing: => A) = {
      (jv1, jv2) match {
        case (JObject(o1), JObject(o2)) => obj(o1)(o2)
        case (JArray(a1), JArray(a2))   => arr(a1)(a2)
        case (JString(s1), JString(s2)) => str(s1)(s2)
        case (JBool(b1), JBool(b2))     => bool(b1)(b2)
        case (JNum(d1), JNum(d2))       => num(d1)(d2)
        case (JNull, JNull)             => nul
        case (JUndefined, JUndefined)   => nul
        case _                          => default
      }
    }
  }

  @inline final def typeIndex(jv: JValue) = jv.typeIndex

  implicit final val jnumOrder: Order[JNum] = new Order[JNum] {
    def order(v1: JNum, v2: JNum) = Ordering.fromInt(v1 numCompare v2)
  }

  implicit final val order: Order[JValue] = new Order[JValue] {
    def order(v1: JValue, v2: JValue) = Ordering.fromInt(v1 compare v2)
  }

  def unsafeInsert(rootTarget: JValue, rootPath: JPath, rootValue: JValue): JValue = {
    // println(s"$rootTarget \\ $rootPath := $rootValue")

    def rec(target: JValue, path: JPath, value: JValue): JValue = {
      if ((target == JNull || target == JUndefined) && path == NoJPath) value
      else {
        def arrayInsert(l: List[JValue], i: Int, rem: JPath, v: JValue): List[JValue] = {
          def update(l: List[JValue], j: Int): List[JValue] = l match {
            case x :: xs => (if (j == i) rec(x, rem, v) else x) :: update(xs, j + 1)
            case Nil     => Nil
          }

          update(l.padTo(i + 1, JUndefined), 0)
        }
        def fail(): Nothing = {
          val msg = s"""
            |JValue insert would overwrite existing data:
            |  $target \\ $path := $value
            |Initial call was
            |  $rootValue \\ $rootPath := $rootValue
            |""".stripMargin.trim
          sys error msg
        }

        target match {
          case obj @ JObject(fields) =>
            path.nodes match {
              case JPathField(name) :: nodes =>
                val (child, rest) = obj.partitionField(name)
                rest + JField(name, rec(child, JPath(nodes), value))

              case JPathIndex(_) :: _ => sys.error("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case Nil                => fail()
            }

          case arr @ JArray(elements) =>
            path.nodes match {
              case JPathIndex(index) :: nodes => JArray(arrayInsert(elements, index, JPath(nodes), value))
              case JPathField(_) :: _         => sys.error("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case Nil                        => fail()
            }

          case JNull | JUndefined =>
            path.nodes match {
              case Nil                => value
              case JPathIndex(_) :: _ => rec(JArray(Nil), path, value)
              case JPathField(_) :: _ => rec(JObject(Nil), path, value)
            }

          case x =>
            // println(s"Target is $x ${x.getClass}")
            fail()
        }
      }
    }

    rec(rootTarget, rootPath, rootValue)
  }
}

case class UndefinedNormalizeError(msg: String) extends Exception(msg)

case object JUndefined extends JValue {
  final def sort: JValue              = this
  protected[json] final def typeIndex = -1
  final def compare(that: JValue)     = typeIndex compare that.typeIndex
}

case object JNull extends JValue {
  final def sort: JValue              = this
  protected[json] final def typeIndex = 0
  final def compare(that: JValue)     = typeIndex compare that.typeIndex
}

sealed trait JBool extends JValue {
  final def sort: JBool               = this
  protected[json] final def typeIndex = 1
  def value: Boolean
}

case object JTrue extends JBool {
  final def value                 = true
  final def compare(that: JValue) = that match {
    case JTrue  => 0
    case JFalse => 1
    case _      => typeIndex compare that.typeIndex
  }
}

case object JFalse extends JBool {
  final def value                 = false
  final def compare(that: JValue) = that match {
    case JTrue  => -1
    case JFalse => 0
    case _      => typeIndex compare that.typeIndex
  }
}

object JBool {
  def apply(value: Boolean): JBool           = if (value) JTrue else JFalse
  def unapply(value: JBool): Option[Boolean] = Some(value.value)
}

sealed trait JNum extends JValue {
  def toBigDecimal: BigDecimal
  def toLong: Long
  def toDouble: Double
  def toRawString: String

  def sort: JNum = this

  // SI-6173: to avoid hashCode on BigDecimal, we use a hardcoded hashcode to
  // ensure JNum("123.45").hashCode == JNum(123.45).hashCode
  override val hashCode = 6173

  protected[json] final def typeIndex = 4

  final def compare(that: JValue): Int = that match {
    case num: JNum => numCompare(num)
    case _         => typeIndex compare that.typeIndex
  }

  protected[json] def numCompare(other: JNum): Int
}

case class JNumStr private[json] (value: String) extends JNum {
  private lazy val dec: BigDecimal = BigDecimal(value)

  final def toBigDecimal: BigDecimal = dec
  final def toLong: Long             = value.toDouble.toLong
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value

  override def equals(other: Any) = other match {
    case JNumLong(n) => value == n.toString
    case num: JNum   => toBigDecimal == num.toBigDecimal
    case _           => false
  }

  protected[json] def numCompare(other: JNum) = toBigDecimal compare other.toBigDecimal
}

case class JNumLong(value: Long) extends JNum {
  final def toBigDecimal: BigDecimal = BigDecimal(value)
  final def toLong: Long             = value
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case JNumBigDec(n) => value == n
    case JNumStr(s)    => value.toString == s
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n
    case JNumDouble(n) => value.toDouble compare n
    case JNumBigDec(n) => BigDecimal(value) compare n
    case _             => toBigDecimal compare other.toBigDecimal
  }
}

case class JNumDouble private[json] (value: Double) extends JNum {
  final def toBigDecimal: BigDecimal = BigDecimal(value)
  final def toLong: Long             = value.toLong
  final def toDouble: Double         = value
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case num: JNum     => toBigDecimal == num.toBigDecimal
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n.toDouble
    case JNumDouble(n) => value compare n
    case JNumBigDec(n) => BigDecimal(value) compare n
    case _             => toBigDecimal compare other.toBigDecimal
  }
}

case class JNumBigDec(value: BigDecimal) extends JNum {
  final def toBigDecimal: BigDecimal = value
  final def toLong: Long             = value.toLong
  final def toDouble: Double         = value.toDouble
  final def toRawString: String      = value.toString

  override def equals(other: Any) = other match {
    case JNumLong(n)   => value == n
    case JNumDouble(n) => value == n
    case JNumBigDec(n) => value == n
    case num: JNum     => value == num.toBigDecimal
    case _             => false
  }

  protected[json] def numCompare(other: JNum) = other match {
    case JNumLong(n)   => value compare n
    case JNumDouble(n) => value compare n
    case JNumBigDec(n) => value compare n
    case _             => value compare other.toBigDecimal
  }
}

case object JNum {
  def apply(value: Double): JValue =
    if (value.isNaN || isInfinite(value)) JUndefined else JNumDouble(value)

  private[json] def apply(value: String): JNum = JNumStr(value)

  def apply(value: Long): JNum = JNumLong(value)

  def apply(value: BigDecimal): JNum = JNumBigDec(value)

  def compare(v1: JNum, v2: JNum) = v1.numCompare(v2)

  def unapply(value: JNum): Option[BigDecimal] = {
    try { Some(value.toBigDecimal) } catch { case _: NumberFormatException => None }
  }
}

case class JString(value: String) extends JValue {
  final def sort: JString = this

  protected[json] final def typeIndex = 5

  final def compare(that: JValue): Int = that match {
    case JString(s) => value compare s
    case _          => typeIndex compare that.typeIndex
  }
}

object JString {
  final def escape(s: String): String = buildString(internalEscape(_, s))

  protected[json] final def internalEscape(sb: StringBuilder, s: String) {
    sb.append('"')
    var i = 0
    val len = s.length
    while (i < len) {
      (s.charAt(i): @switch) match {
        case '"'  => sb.append("\\\"")
        case '\\' => sb.append("\\\\")
        case '\b' => sb.append("\\b")
        case '\f' => sb.append("\\f")
        case '\n' => sb.append("\\n")
        case '\r' => sb.append("\\r")
        case '\t' => sb.append("\\t")
        case c =>
          if (c < ' ')
            sb.append("\\u%04x" format c.toInt)
          else
            sb.append(c)
      }
      i += 1
    }
    sb.append('"')
  }
}

case object JField extends ((String, JValue) => JField) {
  def apply(name: String, value: JValue): JField = (name, value)

  def unapply(value: JField): Option[(String, JValue)] = Some(value)

  implicit final val order: Order[JField] = new Order[JField] {
    def order(f1: JField, f2: JField) = (f1._1 ?|? f2._1) |+| (f1._2 ?|? f2._2)
  }

  implicit final val ordering = order.toScalaOrdering

  def liftFilter(f: JField => Boolean): JValue => JValue =
    (value: JValue) =>
      value match {
        case JObject(fields) => JObject(fields.filter(f))
        case _               => value
    }

  def liftFind(f: JField => Boolean): JValue => Boolean = (jvalue: JValue) => {
    jvalue match {
      case JObject(fields) => !fields.filter(f).isEmpty
      case _               => false
    }
  }

  def liftMap(f: JField => JField): JValue => JValue = (jvalue: JValue) => {
    jvalue match {
      case JObject(fields) => JObject(fields.map(f))
      case _               => jvalue
    }
  }

  def liftCollect(f: PartialFunction[JField, JField]): PartialFunction[JValue, JValue] = new PartialFunction[JValue, JValue] {
    def isDefinedAt(value: JValue): Boolean = !applyOpt(value).isEmpty

    def apply(value: JValue): JValue = applyOpt(value).get

    def applyOpt(value: JValue): Option[JValue] = value match {
      case JObject(fields) =>
        val newFields = fields.collect(f)

        if (newFields.isEmpty) None else Some(JObject(newFields))

      case _ => None
    }
  }
}

case class JObject(fields: Map[String, JValue]) extends JValue {
  assert(fields != null)
  assert(fields.values.forall(_ != null))

  override def toString(): String = "JObject(<%d fields>)" format fields.size

  def get(name: String): JValue = fields.get(name).getOrElse(JUndefined)

  def hasDefinedChild: Boolean = fields.values.exists(_ != JUndefined)

  def +(field: JField): JObject = copy(fields = fields + field)

  def -(name: String): JObject = copy(fields = fields - name)

  def merge(other: JObject): JObject = JObject(Merge.mergeFields(this.fields, other.fields))

  def partitionField(field: String): (JValue, JObject) = {
    (get(field), JObject(fields - field))
  }

  def partition(f: JField => Boolean): (JObject, JObject) = {
    fields.partition(f).bimap(JObject(_), JObject(_))
  }

  def sort: JObject = JObject(fields.filter(_._2 ne JUndefined).map(_.map(_.sort)))

  def mapFields(f: JField => JField) = JObject(fields.map(f))

  def isNested: Boolean = fields.values.exists {
    case _: JArray  => true
    case _: JObject => true
    case _          => false
  }

  protected[json] final def typeIndex = 7

  private def fieldsCmp(m1: Map[String, JValue], m2: Map[String, JValue]): Int = {
    @tailrec def rec(fields: Array[String], i: Int): Int = {
      if (i < fields.length) {
        val key = fields(i)
        val v1  = m1.getOrElse(key, JUndefined)
        val v2  = m2.getOrElse(key, JUndefined)

        if (v1 == JUndefined && v2 == JUndefined) rec(fields, i + 1)
        else if (v1 == JUndefined) 1
        else if (v2 == JUndefined) -1
        else {
          val cres = (v1 compare v2)
          if (cres == 0) rec(fields, i + 1) else cres
        }
      } else {
        0
      }
    }

    val arr: Array[String] = (m1.keySet ++ m2.keySet).toArray
    quickSort(arr)
    rec(arr, 0)
  }

  override def compare(that: JValue): Int = that match {
    case o: JObject => fieldsCmp(fields, o.fields)
    case _          => typeIndex compare that.typeIndex
  }

  private def fieldsEq(m1: Map[String, JValue], m2: Map[String, JValue]): Boolean = {
    (m1.keySet ++ m2.keySet) forall { key =>
      val v1 = m1.getOrElse(key, JUndefined)
      val v2 = m2.getOrElse(key, JUndefined)
      v1 == v2
    }
  }

  override def equals(other: Any) = other match {
    case o: JObject => fieldsEq(fields, o.fields)
    case _          => false
  }
}

case object JObject extends (Map[String, JValue] => JObject) {
  final val empty                          = JObject(Nil)

  implicit val order = Order.orderBy((x: JObject) => x.fields.toVector.sortMe filterNot (_._2 eq JUndefined) sortBy (_._2))

  def apply(fields: Traversable[JField]): JObject = JObject(fields.toMap)
  def apply(fields: JField*): JObject             = JObject(fields.toMap)

  def unapplySeq(value: JValue): Option[Seq[JField]] = value match {
    case JObject(fields) => Some(fields.toSeq)
    case _               => None
  }
}

case class JArray(elements: List[JValue]) extends JValue {
  assert(elements.forall(_ != null))

  override def toString(): String = "JArray(<%d values>)" format elements.length

  def hasDefinedChild: Boolean = elements exists { _ != JUndefined }

  def sort: JArray = JArray(elements.filter(_ ne JUndefined).map(_.sort).sorted)

  // override def apply(i: Int): JValue = elements.lift(i).getOrElse(JUndefined)

  def merge(other: JArray): JArray = JArray(Merge.mergeVals(this.elements, other.elements))

  def isNested: Boolean = elements.exists {
    case _: JArray  => true
    case _: JObject => true
    case _          => false
  }

  protected[json] final def typeIndex = 6

  @tailrec
  private def elementsEq(js1: List[JValue], js2: List[JValue]): Boolean = {
    js1 match {
      case Nil =>
        js2 match {
          case Nil => true
          case _   => false
        }
      case h1 :: t1 =>
        js2 match {
          case Nil      => false
          case h2 :: t2 => if (h1 equals h2) elementsEq(t1, t2) else false
        }
    }
  }

  override def equals(other: Any) = other match {
    case o: JArray => elementsEq(elements, o.elements)
    case _         => false
  }

  @tailrec
  private def elementsCmp(js1: List[JValue], js2: List[JValue]): Int = {
    js1 match {
      case Nil =>
        js2 match {
          case Nil => 0
          case _   => -1
        }

      case h1 :: t1 =>
        js2 match {
          case Nil => 1
          case h2 :: t2 =>
            val i = (h1 compare h2)
            if (i == 0) elementsCmp(t1, t2) else i
        }
    }
  }

  override def compare(other: JValue): Int = other match {
    case o: JArray => elementsCmp(elements, o.elements)
    case o         => JValue.typeIndex(this) compare JValue.typeIndex(o)
  }
}

case object JArray extends (List[JValue] => JArray) {
  final val empty = JArray(Nil)

  final implicit val order: Order[JArray] = Order[List[JValue]].contramap((_: JArray).elements)
  final def apply(vals: JValue*): JArray = JArray(vals.toList)
}
