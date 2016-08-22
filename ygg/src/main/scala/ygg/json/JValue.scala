package ygg.json

import blueeyes._
import scalaz._, Scalaz._, Ordering._
import scala.util.Sorting.quickSort
import java.lang.Double.isInfinite
import JValue.jvalueOrder.order

/**
  * Data type for Json AST.
  */
sealed trait JValue {
  final override def toString: String = this.render
  final override def hashCode: Int = this match {
    case JBool(x)   => x.##
    case JNum(x)    => x.##
    case JString(s) => s.##
    case JArray(x)  => x.##
    case JObject(x) => x.##
    case _          => System identityHashCode this
  }
  final override def equals(other: Any) = other match {
    case x: JValue => (this eq x) || (order(this, x) == EQ)
    case _         => false
  }
}
sealed trait JNum extends JValue
sealed trait JBool extends JValue

final case object JUndefined                          extends JValue
final case object JNull                               extends JValue
final case class JString(value: String)               extends JValue
final case class JObject(fields: Map[String, JValue]) extends JValue
final case class JArray(elements: List[JValue])       extends JValue
final case object JTrue                               extends JBool
final case object JFalse                              extends JBool
final case class JNumLong(n: Long)                    extends JNum
final case class JNumStr(s: String)                   extends JNum
final case class JNumBigDec(b: BigDecimal)            extends JNum

object JValue {
  implicit object jvalueOrder extends Ord[JValue] {
    def order(x: JValue, y: JValue): Ordering = (x, y) match {
      case (JObject(m1), JObject(m2)) => fieldsCompare(m1, m2)
      case (JString(x), JString(y))   => x ?|? y
      case (JNum(x), JNum(y))         => x ?|? y
      case (JArray(x), JArray(y))     => x ?|? y
      case (JBool(x), JBool(y))       => x ?|? y
      case (x, y)                     => x.typeIndex ?|? y.typeIndex
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


  def apply(p: JPath, v: JValue) = JUndefined.set(p, v)

  private def unflattenArray(elements: Seq[JPath -> JValue]): JArray = {
    elements.foldLeft(JArray(Nil)) { (arr, t) =>
      arr.set(t._1, t._2) --> classOf[JArray]
    }
  }

  private def unflattenObject(elements: Seq[JPath -> JValue]): JObject = {
    elements.foldLeft(JObject(Nil)) { (obj, t) =>
      obj.set(t._1, t._2) --> classOf[JObject]
    }
  }

  def unflatten(elements: Seq[JPath -> JValue]): JValue = {
    if (elements.isEmpty) JUndefined
    else {
      val sorted = elements.sortBy(_._1)

      val (xp, xv) = sorted.head

      if (xp == NoJPath && sorted.size == 1) xv
      else if (xp.path.startsWith("[")) unflattenArray(sorted)
      else unflattenObject(sorted)
    }
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

              case JPathIndex(_) :: _ => abort("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case Nil                => fail()
            }

          case arr @ JArray(elements) =>
            path.nodes match {
              case JPathIndex(index) :: nodes => JArray(arrayInsert(elements, index, JPath(nodes), value))
              case JPathField(_) :: _         => abort("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
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
object JBool {
  def apply(value: Boolean): JBool         = if (value) JTrue else JFalse
  def unapply(value: JBool): Some[Boolean] = Some(value eq JTrue)
}

final object JNum {
  implicit val jnumOrder: Ord[JNum] = Order.order[JNum] {
    case (JNumLong(x), JNumLong(y))     => x ?|? y
    case (JNumBigDec(x), JNumBigDec(y)) => x ?|? y
    case (JNum(x), JNum(y))             => x ?|? y
  }

  def apply(value: Double): JValue             = if (value.isNaN || isInfinite(value)) JUndefined else apply(value.toString)
  def apply(value: String): JValue             = if (value == null) JUndefined else JNumStr(value)
  def apply(value: Int): JNum                  = JNumLong(value.toLong)
  def apply(value: Long): JNum                 = JNumLong(value)
  def apply(value: BigDecimal): JNum           = JNumBigDec(value)
  def unapply(value: JNum): Option[BigDecimal] = Try(value.toBigDecimal).toOption
}

final case class JField(name: String, value: JValue) extends Product2[String, JValue] {
  def _1                        = name
  def _2                        = value
  def toTuple: String -> JValue = name -> value
  def isUndefined               = value == JUndefined
}
final object JField {
  def apply(x: JFieldTuple): JField = JField(x._1, x._2)

  implicit def liftTuple(x: JFieldTuple): JField = apply(x)

  def liftFilter(f: JField => Boolean): JValue => JValue = {
    case JObjectFields(fields) => JObject(fields filter f)
    case value                 => value
  }
  def liftFind(f: JField => Boolean): JValue => Boolean = {
    case JObjectFields(fields) => fields exists f
    case _                     => false
  }
  def liftMap(f: JField => JField): JValue => JValue = {
    case JObjectFields(fields) => JObject(fields map f)
    case value                 => value
  }
  def liftCollect(f: PartialFunction[JField, JField]): PartialFunction[JValue, JValue] = {
    case JObjectFields(fields) if fields exists f.isDefinedAt => JObject(fields collect f)
  }
}
object JObjectFields {
  def unapply(m: JObject): Some[Vector[JField]] = Some(m.sortedFields)
}

final object JObject {
  val empty = JObject(Nil)

  def apply(fields: Traversable[JField]): JObject      = JObject(fields.map(_.toTuple).toMap)
  def apply(fields: JField*): JObject                  = JObject(fields.map(_.toTuple).toMap)
  def unapplySeq(value: JObject): Some[Vector[JField]] = Some(value.sortedFields)
}

final object JArray extends (List[JValue] => JArray) {
  val empty = JArray(Nil)

  def apply(vals: JValue*): JArray = JArray(vals.toList)
}
