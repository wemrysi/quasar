package ygg.table

import ygg.common._
import scalaz._, Scalaz._, Ordering._
import ygg.json._
import ygg.macros.Spire._

sealed trait RValue
final case class RObject(fields: Map[String, RValue]) extends RValue
final case class RArray(elements: Vec[RValue]) extends RValue

sealed trait CValue extends RValue {
  type Type <: CType
  def cType: Type
}
sealed trait CWrappedValue[A] extends CValue {
  type Type = CValueType[A]

  def value: A
  def cType: Type = ((this: CWrappedValue[_]) match {
    case CArray(_, tpe) => tpe
    case CString(_)     => CString
    case CBoolean(_)    => CBoolean
    case CLong(_)       => CLong
    case CDouble(_)     => CDouble
    case CNum(_)        => CNum
    case CDate(_)       => CDate
    case CPeriod(_)     => CPeriod
  }).asInstanceOf[Type]
}
sealed trait CNumericValue[A] extends CWrappedValue[A] {
  final override def hashCode = 12

  def toCNum: CNum = CNum(
    (this: CNumericValue[_]) match {
      case CLong(x)   => decimal(x)
      case CDouble(x) => decimal(x.toString)
      case CNum(x)    => x
    }
  )
}
sealed trait CType extends Serializable {
  def readResolve(): CType

  def typeIndex: Int = this match {
    case CUndefined    => 0
    case CBoolean      => 1
    case CString       => 2
    case CLong         => 4
    case CDouble       => 6
    case CNum          => 7
    case CEmptyObject  => 8
    case CEmptyArray   => 9
    case CArrayType(_) => 10 // TODO: Should this account for the element type?
    case CNull         => 11
    case CDate         => 12
    case CPeriod       => 13
  }
}

sealed abstract class CNullType extends CType with CValue {
  type Type = CNullType
  def cType: CNullType = this
}
sealed abstract class CValueType[A: CTag] extends CType {
  val classTag: CTag[A] = implicitly[CTag[A]]

  def readResolve(): CValueType[A]
  def apply(a: A): CWrappedValue[A]
  def order(a: A, b: A): Cmp
}
sealed abstract class CNumericType[A: CTag] extends CValueType[A] {
  def bigDecimalFor(a: A): BigDecimal
}

object RValue {
  implicit class RValueOps(private val self: RValue) {
    def toJValue: JValue = self match {
      case RObject(fields) => JObject(fields mapValues (_.toJValue) toMap)
      case RArray(xs)      => JArray(xs map (_.toJValue))
      case CString(x)      => JString(x)
      case CBoolean(x)     => JBool(x)
      case CLong(x)        => JNum(x)
      case CDouble(x)      => JNum(x)
      case CNum(x)         => JNum(x)
      case CEmptyArray     => jarray()
      case CEmptyObject    => jobject()
      case CNull           => JNull
      case CUndefined      => JUndefined
      case CDate(x)        => JString(x.toString)
      case CPeriod(x)      => JString(x.toString)
      case CArray(_, _)    => abort("HOMOGENEOUS ARRAY ESCAPING! ALERT! ALERT!")
    }
    def \(name: String): RValue = self match {
      case RObject(fields) => fields(name)
      case _               => CUndefined
    }

    def unsafeInsert(path: CPath, value: RValue): RValue = RValue.unsafeInsert(self, path, value)

    def flattenWithPath: Vec[CPath.AndValue] = {
      def flatten0(path: CPath)(value: RValue): Vec[CPath.AndValue] = value match {
        case RObject(fields) if fields.isEmpty => Vec(path -> CEmptyObject)
        case RArray(elems) if elems.isEmpty    => Vec(path -> CEmptyArray)
        case RObject(fields)                   => fields.toVector flatMap { case (name, value) => flatten0(path \ name)(value) }
        case RArray(elems)                     => elems.zipWithIndex flatMap { case (value, idx) => flatten0(path \ idx)(value) }
        case v: CValue                         => Vec(path -> v)
      }

      flatten0(CPath.Identity)(self)
    }
  }
  def fromJValue(jv: JValue): RValue = jv match {
    case JObject(fields)  => RObject(fields mapValues fromJValue toMap)
    case JArray(elements) => RArray(elements map fromJValue)
    case JString(s)       => CString(s)
    case JBool(b)         => CBoolean(b)
    case JNull            => CNull
    case JNum(bd)         => fromJNum(bd)
  }
  def fromJNum(d: BigDecimal): CValue = {
    def asLong   = Try(CLong(d.toLongExact)).toOption
    def asDouble = Try(decimal(d.toDouble.toString)).toOption collect { case `d` => CDouble(d.toDouble) }

    asLong orElse asDouble getOrElse CNum(d)
  }

  def unsafeInsert(rootTarget: RValue, rootPath: CPath, rootValue: RValue): RValue = {
    def rec(target: RValue, path: CPath, value: RValue): RValue = {
      if ((target == CNull || target == CUndefined) && path == CPath.Identity) value
      else {
        def arrayInsert(l: Vec[RValue], i: Int, rem: CPath, v: RValue): Vec[RValue] = {
          def update(l: Vec[RValue], j: Int): Vec[RValue] = l match {
            case x +: xs => (if (j == i) rec(x, rem, v) else x) +: update(xs, j + 1)
            case Seq()   => Vec()
          }

          update(l.padTo(i + 1, CUndefined), 0)
        }

        target match {
          case obj @ RObject(fields) =>
            path.nodes match {
              case CPathField(name) +: nodes =>
                val (child, rest) = (fields.get(name).getOrElse(CUndefined), fields - name)
                RObject(rest + (name -> rec(child, CPath(nodes), value)))

              case CPathIndex(_) +: _ => abort("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case _ =>
                abort(
                  "RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                    " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
            }

          case arr @ RArray(elements) =>
            path.nodes match {
              case CPathIndex(index) +: nodes => RArray(arrayInsert(elements, index, CPath(nodes), value))
              case CPathField(_) +: _         => abort("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
              case _ =>
                abort(
                  "RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                    " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
            }

          case CNull | CUndefined =>
            path.nodes match {
              case Vec()              => value
              case CPathIndex(_) +: _ => rec(RArray.empty, path, value)
              case CPathField(_) +: _ => rec(RObject.empty, path, value)
              case CPathArray +: _    => ???
              case CPathMeta(_) +: _  => ???
            }

          case x =>
            abort(
              "RValue insert would overwrite existing data: " + x + " cannot be updated to " + value + " at " + path +
                " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
        }
      }
    }

    rec(rootTarget, rootPath, rootValue)
  }
}

object RObject {
  val empty: RObject                           = new RObject(Map.empty)
  def apply(fields: (String, RValue)*): RValue = new RObject(fields.toMap)
}
object RArray {
  val empty: RArray                    = new RArray(Vec())
  def apply(elements: RValue*): RValue = new RArray(elements.toVector)
}
object CWrappedValue {
  def unapply[A](x: CWrappedValue[A]): Some[A] = Some(x.value)
}

object CValue {
  def apply[A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  implicit val CValueOrder: Ord[CValue] = Ord order {
    case (CString(as), CString(bs))                                                   => as ?|? bs
    case (CBoolean(ab), CBoolean(bb))                                                 => ab ?|? bb
    case (CLong(al), CLong(bl))                                                       => al ?|? bl
    case (CDouble(ad), CDouble(bd))                                                   => ad ?|? bd
    case (CNum(an), CNum(bn))                                                         => fromInt(an compare bn)
    case (CDate(ad), CDate(bd))                                                       => fromInt(ad compareTo bd)
    case (CPeriod(ad), CPeriod(bd))                                                   => ad.toDuration ?|? bd.toDuration
    case (CArray(as, CArrayType(atpe)), CArray(bs, CArrayType(btpe))) if atpe == btpe => as.toStream.map(x => atpe(x): CValue) ?|? bs.toStream.map(x => btpe(x))
    case (a: CNumericValue[_], b: CNumericValue[_])                                   => (a.toCNum: CValue) ?|? b.toCNum
    case (a, b)                                                                       => (a.cType: CType) ?|? b.cType
  }
}

object CType {
  def readResolve() = CType
  def of(v: CValue): CType = v.cType

  def canCompare(t1: CType, t2: CType): Boolean = (t1 == t2) || {
    (t1, t2) match {
      case (_: CNumericType[_], _: CNumericType[_]) => true
      case _                                        => false
    }
  }

  def unify(t1: CType, t2: CType): Option[CType] = (t1, t2) match {
    case _ if t1 == t2                                    => Some(t1)
    case (CLong | CDouble | CNum, CLong | CDouble | CNum) => Some(CNum)
    case (CArrayType(et1), CArrayType(et2))               => unify(et1, et2) collect { case t: CValueType[_] => CArrayType(t) }
    case _                                                => None
  }

  @inline
  final def forJValue(jval: JValue): Option[CType] = jval match {
    case JBool(_)      => Some(CBoolean)
    case JNum(d)       => Some(RValue.fromJNum(d).cType)
    case JString(_)    => Some(CString)
    case JNull         => Some(CNull)
    case JArray.empty  => Some(CEmptyArray)
    case JObject.empty => Some(CEmptyObject)
    case _             => None // TODO Allow homogeneous JArrays -> CType
  }

  implicit val CTypeOrder: Ord[CType] = Order order {
    case (CArrayType(t1), CArrayType(t2)) => (t1: CType) ?|? t2
    case (x, y)                           => x.typeIndex ?|? y.typeIndex
  }
}

object CValueType {
  def apply[A](implicit A: CValueType[A]): CValueType[A]          = A
  def apply[A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  // These let us do, def const[A: CValueType](a: A): CValue = CValueType[A](a)
  implicit def string: CValueType[String]                 = CString
  implicit def boolean: CValueType[Boolean]               = CBoolean
  implicit def long: CValueType[Long]                     = CLong
  implicit def double: CValueType[Double]                 = CDouble
  implicit def bigDecimal: CValueType[BigDecimal]         = CNum
  implicit def dateTime: CValueType[DateTime]             = CDate
  implicit def period: CValueType[Period]                 = CPeriod
  implicit def array[A](implicit elemType: CValueType[A]) = CArrayType(elemType)
}

//
// Homogeneous arrays
//
case class CArray[A](value: Array[A], arrayType: CArrayType[A]) extends CWrappedValue[Array[A]] {
  private final def leafEquiv[A](as: Array[A], bs: Array[A]): Boolean = (as.length == bs.length) && {
    cforRange(0 until as.length){ i =>
      if (as(i) != bs(i))
        return false
    }
    true
  }

  private final def equiv(a: Any, b: Any, elemType: CValueType[_]): Boolean = elemType match {
    case CBoolean             => leafEquiv(a.asInstanceOf[Array[Boolean]], b.asInstanceOf[Array[Boolean]])
    case CLong                => leafEquiv(a.asInstanceOf[Array[Long]], b.asInstanceOf[Array[Long]])
    case CDouble              => leafEquiv(a.asInstanceOf[Array[Double]], b.asInstanceOf[Array[Double]])
    case CArrayType(elemType) => (a.asInstanceOf[Array[Array[_]]] corresponds b.asInstanceOf[Array[Array[_]]])(equiv(_, _, elemType))
    case _                    => leafEquiv(a.asInstanceOf[Array[AnyRef]], b.asInstanceOf[Array[AnyRef]])
  }

  override def equals(that: Any): Boolean = that match {
    case v @ CArray(_, thatType) if thatType == arrayType => equiv(value, v.value, arrayType.elemType)
    case _                                                => false
  }
}

case object CArray {
  def apply[A](as: Array[A])(implicit elemType: CValueType[A]): CArray[A] =
    CArray(as, CArrayType(elemType))
}

case class CArrayType[A](elemType: CValueType[A]) extends CValueType[Array[A]]()(elemType.classTag.wrap) {
  type tpe = A

  def readResolve() = CArrayType(elemType.readResolve())

  def apply(value: Array[A]) = CArray(value, this)

  def order(as: Array[A], bs: Array[A]): Ordering =
    (as, bs).zipped map ((x, y) => elemType.order(x, y)) find (_ != EQ) getOrElse (as.length ?|? bs.length)
}

//
// Strings
//
case class CString(value: String) extends CWrappedValue[String]

case object CString extends CValueType[String] {
  def readResolve()                 = CString
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
}

//
// Booleans
//
sealed abstract class CBoolean(val value: Boolean) extends CWrappedValue[Boolean]

case object CTrue  extends CBoolean(true)
case object CFalse extends CBoolean(false)

object CBoolean extends CValueType[Boolean] {
  def apply(value: Boolean)           = if (value) CTrue else CFalse
  def unapply(cbool: CBoolean)        = Some(cbool.value)
  def readResolve()                   = CBoolean
  def order(v1: Boolean, v2: Boolean) = booleanInstance.order(v1, v2)
}

//
// Numerics
//
case class CLong(value: Long) extends CNumericValue[Long]

case object CLong extends CNumericType[Long] {
  def readResolve()             = CLong
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def bigDecimalFor(v: Long)    = decimal(v)
}

case class CDouble(value: Double) extends CNumericValue[Double]

case object CDouble extends CNumericType[Double] {
  def readResolve()                 = CDouble
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def bigDecimalFor(v: Double)      = decimal(v.toString)
}

case class CNum(value: BigDecimal) extends CNumericValue[BigDecimal]

case object CNum extends CNumericType[BigDecimal] {
  def readResolve()                         = CNum
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalOrder.order(v1, v2)
  def bigDecimalFor(v: BigDecimal)          = v
}

//
// Dates and Periods
//
case class CDate(value: DateTime) extends CWrappedValue[DateTime]

case object CDate extends CValueType[DateTime] {
  def readResolve()                     = CDate
  def order(v1: DateTime, v2: DateTime) = ???
}

case class CPeriod(value: Period) extends CWrappedValue[Period]

case object CPeriod extends CValueType[Period] {
  def readResolve()                 = CPeriod
  def order(v1: Period, v2: Period) = ???
}

//
// Null / Undef
//
case object CNull extends CNullType {
  def readResolve() = CNull
}

case object CEmptyObject extends CNullType {
  def readResolve() = CEmptyObject
}
case object CEmptyArray extends CNullType {
  def readResolve() = CEmptyArray
}
case object CUndefined extends CNullType {
  def readResolve() = CUndefined
}
