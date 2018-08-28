/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil.table

import quasar.blueeyes._, json._
import quasar.precog._
import quasar.precog.common._
import quasar.precog.util._
import qdata.time.{DateTimeInterval, OffsetDate}

import scalaz.Semigroup

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

import scala.annotation.tailrec
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.specialized

sealed trait Column {
  def isDefinedAt(row: Int): Boolean
  def |>(f1: CF1): Option[Column] = f1(this)

  val tpe: CType
  def jValue(row: Int): JValue
  def cValue(row: Int): CValue
  def strValue(row: Int): String

  def toString(row: Int): String     = if (isDefinedAt(row)) strValue(row) else "(undefined)"
  def toString(range: Range): String = range.map(toString(_: Int)).mkString("(", ",", ")")

  def definedAt(from: Int, to: Int): BitSet =
    BitSetUtil.filteredRange(from, to)(isDefinedAt)

  def rowEq(row1: Int, row2: Int): Boolean
  def rowCompare(row1: Int, row2: Int): Int
}

trait HomogeneousArrayColumn[@specialized(Boolean, Long, Double) A] extends Column with (Int => Array[A]) { self =>
  val tpe: CArrayType[A]
  def apply(row: Int): Array[A]
  def isDefinedAt(row: Int): Boolean

  def rowEq(row1: Int, row2: Int): Boolean = {
    if (!isDefinedAt(row1)) return !isDefinedAt(row2)
    if (!isDefinedAt(row2)) return false

    val a1 = apply(row1)
    val a2 = apply(row2)
    if (a1.length != a2.length) return false
    val n = a1.length
    var i = 0
    while (i < n) {
      if (a1(i) != a2(i)) return false
      i += 1
    }
    true
  }

  def rowCompare(row1: Int, row2: Int): Int = {
    sys.error("...")
  }


  def leafTpe: CValueType[_] = {
    @tailrec def loop(a: CValueType[_]): CValueType[_] = a match {
      case CArrayType(elemType) => loop(elemType)
      case vType                => vType
    }

    loop(tpe)
  }

  override def jValue(row: Int)   = tpe.jValueFor(this(row))
  override def cValue(row: Int)   = tpe(this(row))
  override def strValue(row: Int) = this(row) mkString ("[", ",", "]")

  /**
    * Returns a new Column that selects the `i`-th element from the
    * underlying arrays.
    */
  def select(i: Int) = HomogeneousArrayColumn.select(this, i)
}

object HomogeneousArrayColumn {
  def apply[A: CValueType](f: PartialFunction[Int, Array[A]]): HomogeneousArrayColumn[A] =
    new HomogeneousArrayColumn[A] {
      val tpe: CArrayType[A]    = implicitly
      def isDefinedAt(row: Int) = f isDefinedAt row
      def apply(row: Int)       = f(row)
    }

  def unapply[A](col: HomogeneousArrayColumn[A]): Option[CValueType[A]] =
    Some(col.tpe.elemType)

  @inline
  private[table] def select[A](col: HomogeneousArrayColumn[A], i: Int) = col match {
    case col @ HomogeneousArrayColumn(CString) =>
      new StrColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): String = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CBoolean) =>
      new BoolColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): Boolean = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CLong) =>
      new LongColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): Long = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CDouble) =>
      new DoubleColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): Double = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CNum) =>
      new NumColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): BigDecimal = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(COffsetDateTime) =>
      new OffsetDateTimeColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): OffsetDateTime = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(COffsetTime) =>
      new OffsetTimeColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): OffsetTime = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(COffsetDate) =>
      new OffsetDateColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): OffsetDate = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CLocalDateTime) =>
      new LocalDateTimeColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): LocalDateTime = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CLocalTime) =>
      new LocalTimeColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): LocalTime = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CLocalDate) =>
      new LocalDateColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): LocalDate = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CInterval) =>
      new IntervalColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): DateTimeInterval = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(cType: CArrayType[a]) =>
      new HomogeneousArrayColumn[a] {
        val tpe = cType
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): Array[a] = col(row)(i)
      }
  }
}

trait BoolColumn extends Column with (Int => Boolean) {
  def apply(row: Int): Boolean
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  def asBitSet(undefinedVal: Boolean, size: Int): BitSet = {
    val back = new BitSet(size)
    var i = 0
    while (i < size) {
      val b =
        if (!isDefinedAt(i))
          undefinedVal
        else
          apply(i)

      back.set(i, b)
      i += 1
    }
    back
  }

  override val tpe = CBoolean
  override def jValue(row: Int)           = JBool(this(row))
  override def cValue(row: Int)           = CBoolean(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString                   = "BoolColumn"
}

object BoolColumn {
  def True(definedAt: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = true
  }

  def False(definedAt: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = false
  }

  def Either(definedAt: BitSet, values: BitSet) = new BitsetColumn(definedAt) with BoolColumn {
    def apply(row: Int) = values(row)
  }
}

trait LongColumn extends Column with (Int => Long) {
  def apply(row: Int): Long
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe = CLong
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CLong(this(row))
  override def strValue(row: Int): String = String.valueOf(jValue(row))
  override def toString                   = "LongColumn"
}

trait DoubleColumn extends Column with (Int => Double) {
  def apply(row: Int): Double
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe = CDouble
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(JValue.coerceNumerics(jValue(row)))
  override def toString                   = "DoubleColumn"
}

trait NumColumn extends Column with (Int => BigDecimal) {
  def apply(row: Int): BigDecimal
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe = CNum
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CNum(this(row))
  override def strValue(row: Int): String = JValue.coerceNumerics(jValue(row)).toString
  override def toString                   = "NumColumn"
}

trait StrColumn extends Column with (Int => String) {
  def apply(row: Int): String
  def rowEq(row1: Int, row2: Int): Boolean = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compareTo apply(row2)

  override val tpe = CString
  override def jValue(row: Int)           = JString(this(row))
  override def cValue(row: Int)           = CString(this(row))
  override def strValue(row: Int): String = this(row)
  override def toString                   = "StrColumn"
}

trait OffsetDateTimeColumn extends Column with (Int => OffsetDateTime) {
  def apply(row: Int): OffsetDateTime
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = COffsetDateTime
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = COffsetDateTime(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "OffsetDateTimeColumn"
}

trait OffsetTimeColumn extends Column with (Int => OffsetTime) {
  def apply(row: Int): OffsetTime
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = COffsetTime
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = COffsetTime(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "OffsetTimeColumn"
}

trait OffsetDateColumn extends Column with (Int => OffsetDate) {
  def apply(row: Int): OffsetDate
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = COffsetDate
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = COffsetDate(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "OffsetDateColumn"
}

trait LocalDateTimeColumn extends Column with (Int => LocalDateTime) {
  def apply(row: Int): LocalDateTime
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = CLocalDateTime
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CLocalDateTime(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "LocalDateTimeColumn"
}

trait LocalTimeColumn extends Column with (Int => LocalTime) {
  def apply(row: Int): LocalTime
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = CLocalTime
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CLocalTime(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "LocalTimeColumn"
}

trait LocalDateColumn extends Column with (Int => LocalDate) {
  def apply(row: Int): LocalDate
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = CLocalDate
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CLocalDate(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "LocalDateColumn"
}

trait IntervalColumn extends Column with (Int => DateTimeInterval) {
  def apply(row: Int): DateTimeInterval
  def rowEq(row1: Int, row2: Int): Boolean  = apply(row1) == apply(row2)
  // TODO: fix this
  def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare periods.")

  override val tpe                        = CInterval
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CInterval(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "IntervalColumn"
}

trait EmptyArrayColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean  = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe                          = CEmptyArray
  override def jValue(row: Int)             = JArray(Nil)
  override def cValue(row: Int)             = CEmptyArray
  override def strValue(row: Int): String   = "[]"
  override def toString                     = "EmptyArrayColumn"
}
object EmptyArrayColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyArrayColumn
}

trait EmptyObjectColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean  = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe = CEmptyObject
  override def jValue(row: Int)           = JObject(Nil)
  override def cValue(row: Int)           = CEmptyObject
  override def strValue(row: Int): String = "{}"
  override def toString                   = "EmptyObjectColumn"
}

object EmptyObjectColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyObjectColumn
}

trait NullColumn extends Column {
  def rowEq(row1: Int, row2: Int): Boolean  = true
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe = CNull
  override def jValue(row: Int)           = JNull
  override def cValue(row: Int)           = CNull
  override def strValue(row: Int): String = "null"
  override def toString                   = "NullColumn"
}
object NullColumn {
  def apply(definedAt: BitSet) = {
    new BitsetColumn(definedAt) with NullColumn
  }
}

object UndefinedColumn {
  def apply(col: Column) = new Column {
    def rowEq(row1: Int, row2: Int): Boolean  = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare undefined values.")
    def isDefinedAt(row: Int)                 = false
    val tpe                                   = col.tpe
    def jValue(row: Int)                      = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int)                      = CUndefined
    def strValue(row: Int)                    = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }

  val raw = new Column {
    def rowEq(row1: Int, row2: Int): Boolean  = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def rowCompare(row1: Int, row2: Int): Int = sys.error("Cannot compare undefined values.")
    def isDefinedAt(row: Int)                 = false
    val tpe                                   = CUndefined
    def jValue(row: Int)                      = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
    def cValue(row: Int)                      = CUndefined
    def strValue(row: Int)                    = sys.error("Values in undefined columns SHOULD NOT BE ACCESSED")
  }
}

case class MmixPrng(_seed: Long) {
  private var seed: Long = _seed

  def nextLong(): Long = {
    val next: Long = 6364136223846793005L * seed + 1442695040888963407L
    seed = next
    next
  }

  def nextDouble(): Double = {
    val n = nextLong()
    (n >>> 11) * 1.1102230246251565e-16
  }
}

object Column {
  def rowOrder(col: Column): spire.algebra.Order[Int] = new spire.algebra.Order[Int] {
    def compare(i: Int, j: Int): Int = {
      if (col.isDefinedAt(i)) {
        if (col.isDefinedAt(j)) {
          col.rowCompare(i, j)
        } else 1
      } else if (col.isDefinedAt(j)) -1
      else 0
    }
  }

  @inline def const(cv: CValue): Column = cv match {
    case CBoolean(v)                         => const(v)
    case CLong(v)                            => const(v)
    case CDouble(v)                          => const(v)
    case CNum(v)                             => const(v)
    case CString(v)                          => const(v)
    case COffsetDateTime(v)                  => const(v)
    case COffsetDate(v)                      => const(v)
    case COffsetTime(v)                      => const(v)
    case CLocalDateTime(v)                   => const(v)
    case CLocalDate(v)                       => const(v)
    case CLocalTime(v)                       => const(v)
    case CInterval(v)                        => const(v)
    case CArray(v, t @ CArrayType(elemType)) => const(v)(elemType)
    case CEmptyObject                        => new InfiniteColumn with EmptyObjectColumn
    case CEmptyArray                         => new InfiniteColumn with EmptyArrayColumn
    case CNull                               => new InfiniteColumn with NullColumn
    case CUndefined                          => UndefinedColumn.raw
    case _                                   => sys.error(s"Unexpected arg $cv")
  }

  @inline def uniformDistribution(init: MmixPrng): (Column, MmixPrng) = {
    val col = new InfiniteColumn with DoubleColumn {
      var memo = mutable.ArrayBuffer.empty[Double]

      def apply(row: Int) = {
        val maxRowComputed = memo.length

        if (row < maxRowComputed) {
          memo(row)
        } else {
          var i   = maxRowComputed
          var res = 0d

          while (i <= row) {
            res = init.nextDouble()
            memo += res
            i += 1
          }

          res
        }
      }
    }
    (col, init)
  }

  @inline def const(v: Boolean) = new InfiniteColumn with BoolColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Long) = new InfiniteColumn with LongColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Double) = new InfiniteColumn with DoubleColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: BigDecimal) = new InfiniteColumn with NumColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: String) = new InfiniteColumn with StrColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: OffsetDateTime) = new InfiniteColumn with OffsetDateTimeColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: OffsetTime) = new InfiniteColumn with OffsetTimeColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: OffsetDate) = new InfiniteColumn with OffsetDateColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: LocalDateTime) = new InfiniteColumn with LocalDateTimeColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: LocalTime) = new InfiniteColumn with LocalTimeColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: LocalDate) = new InfiniteColumn with LocalDateColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: DateTimeInterval) = new InfiniteColumn with IntervalColumn {
    def apply(row: Int) = v
  }

  @inline def const[@specialized(Boolean, Long, Double) A: CValueType](v: Array[A]) = new InfiniteColumn with HomogeneousArrayColumn[A] {
    val tpe = CArrayType(CValueType[A])
    def apply(row: Int) = v
  }

  def lift(col: Column): HomogeneousArrayColumn[_] = col match {
    case col: BoolColumn                => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: LongColumn                => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: DoubleColumn              => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: NumColumn                 => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: StrColumn                 => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: OffsetDateTimeColumn      => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: OffsetTimeColumn          => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: OffsetDateColumn          => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: LocalDateTimeColumn       => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: LocalTimeColumn           => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: LocalDateColumn           => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: IntervalColumn            => HomogeneousArrayColumn { case row if col isDefinedAt row => Array(col(row)) }
    case col: HomogeneousArrayColumn[a] =>
      new HomogeneousArrayColumn[Array[a]] {
        val tpe = CArrayType(col.tpe)
        def isDefinedAt(row: Int)            = col.isDefinedAt(row)
        def apply(row: Int): Array[Array[a]] = Array(col(row))(col.tpe.classTag)
      }
    case _ => sys.error("Cannot lift non-value column.")
  }

  object unionRightSemigroup extends Semigroup[Column] {
    def append(c1: Column, c2: => Column): Column = {
      cf.util.UnionRight(c1, c2) getOrElse {
        sys.error("Illgal attempt to merge columns of dissimilar type: " + c1.tpe + "," + c2.tpe)
      }
    }
  }

  def isDefinedAt(cols: Array[Column], row: Int): Boolean = {
    var i = 0
    while (i < cols.length && !cols(i).isDefinedAt(row)) {
      i += 1
    }
    i < cols.length
  }

  def isDefinedAtAll(cols: Array[Column], row: Int): Boolean =
    cols.length > 0 && cols.forall(_ isDefinedAt row)
}

trait ArrayColumn[@specialized(Boolean, Long, Double) A] extends BitsetColumn with Column {
  def update(row: Int, value: A): this.type
  def clear(row: Int): Unit
  def resize(size: Int): ArrayColumn[A]
}

object ArrayColumn {
  private[table] def resizeArray[A](arr: Array[A], size: Int)(implicit A: ClassTag[A]): Array[A] = {
    val newArr = new Array[A](size)
    System.arraycopy(arr, 0, newArr, 0, Math.min(size, arr.length))
    newArr
  }
}

trait AccessibleArrayColumn[@specialized(Boolean, Long, Double) A] extends ArrayColumn[A] {
  def values: Array[A]
}

class ArrayHomogeneousArrayColumn[@specialized(Boolean, Long, Double) A](val defined: BitSet, val values: Array[Array[A]])(implicit val tpe: CArrayType[A])
    extends BitsetColumn(defined)
    with HomogeneousArrayColumn[A]
    with ArrayColumn[Array[A]] {

  def apply(row: Int) = values(row)

  def update(row: Int, value: Array[A]) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Array[A]] = {
    implicit val ct: ClassTag[Array[A]] = tpe.classTag
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayHomogeneousArrayColumn(newDefined, newValues)
  }
}

object ArrayHomogeneousArrayColumn {
  def apply[@specialized(Boolean, Long, Double) A: CValueType](values: Array[Array[A]]) =
    new ArrayHomogeneousArrayColumn(BitSetUtil.range(0, values.length), values)(CArrayType(CValueType[A]))
  def apply[@specialized(Boolean, Long, Double) A: CValueType](defined: BitSet, values: Array[Array[A]]) =
    new ArrayHomogeneousArrayColumn(defined.copy, values)(CArrayType(CValueType[A]))
  def empty[@specialized(Boolean, Long, Double) A](size: Int)(implicit elemType: CValueType[A]): ArrayHomogeneousArrayColumn[A] = {
    // this *is* used by the compiler to make the new array,
    // by generating a `ClassTag[Array[A]]`.
    implicit val m: ClassTag[A] = elemType.classTag

    val _ = m

    new ArrayHomogeneousArrayColumn(new BitSet, new Array[Array[A]](size))(CArrayType(elemType))
  }
}

class ArrayBoolColumn(val defined: BitSet, val values: BitSet) extends BitsetColumn(defined) with ArrayColumn[Boolean] with BoolColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Boolean) = {
    defined.set(row)
    if (value) values.set(row) else values.clear(row)
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Boolean] = {
    val newValues = values.resizeBits(size)
    val newDefined = defined.resizeBits(size)
    new ArrayBoolColumn(newDefined, newValues)
  }

}

object ArrayBoolColumn {
  def apply(defined: BitSet, values: BitSet) =
    new ArrayBoolColumn(defined.copy, values.copy)
  def apply(defined: BitSet, values: Array[Boolean]) =
    new ArrayBoolColumn(defined.copy, BitSetUtil.filteredRange(0, values.length)(values))
  def apply(values: Array[Boolean]) = {
    val d = BitSetUtil.range(0, values.length)
    val v = BitSetUtil.filteredRange(0, values.length)(values)
    new ArrayBoolColumn(d, v)
  }

  def empty(size: Int): ArrayBoolColumn =
    new ArrayBoolColumn(new BitSet(size), new BitSet(size))
}

class ArrayLongColumn(val defined: BitSet, val values: Array[Long]) extends BitsetColumn(defined) with AccessibleArrayColumn[Long] with LongColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Long) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Long] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayLongColumn(newDefined, newValues)
  }

}

object ArrayLongColumn {
  def apply(values: Array[Long]) =
    new ArrayLongColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[Long]) =
    new ArrayLongColumn(defined.copy, values)
  def empty(size: Int): ArrayLongColumn =
    new ArrayLongColumn(new BitSet, new Array[Long](size))
}

class ArrayDoubleColumn(val defined: BitSet, val values: Array[Double]) extends BitsetColumn(defined) with AccessibleArrayColumn[Double] with DoubleColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: Double) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Double] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayDoubleColumn(newDefined, newValues)
  }

}

object ArrayDoubleColumn {
  def apply(values: Array[Double]) =
    new ArrayDoubleColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[Double]) =
    new ArrayDoubleColumn(defined.copy, values)
  def empty(size: Int): ArrayDoubleColumn =
    new ArrayDoubleColumn(new BitSet, new Array[Double](size))
}

class ArrayNumColumn(val defined: BitSet, val values: Array[BigDecimal]) extends BitsetColumn(defined) with AccessibleArrayColumn[BigDecimal] with NumColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: BigDecimal) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[BigDecimal] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayNumColumn(newDefined, newValues)
  }

}

object ArrayNumColumn {
  def apply(values: Array[BigDecimal]) =
    new ArrayNumColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[BigDecimal]) =
    new ArrayNumColumn(defined.copy, values)
  def empty(size: Int): ArrayNumColumn =
    new ArrayNumColumn(new BitSet, new Array[BigDecimal](size))
}

class ArrayStrColumn(val defined: BitSet, val values: Array[String]) extends BitsetColumn(defined) with AccessibleArrayColumn[String] with StrColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: String) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[String] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayStrColumn(newDefined, newValues)
  }

}

object ArrayStrColumn {
  def apply(values: Array[String]) =
    new ArrayStrColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[String]) =
    new ArrayStrColumn(defined.copy, values)
  def empty(size: Int): ArrayStrColumn =
    new ArrayStrColumn(new BitSet, new Array[String](size))
}

class ArrayOffsetDateTimeColumn(val defined: BitSet, val values: Array[OffsetDateTime]) extends BitsetColumn(defined) with AccessibleArrayColumn[OffsetDateTime] with OffsetDateTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetDateTime) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[OffsetDateTime] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayOffsetDateTimeColumn(newDefined, newValues)
  }

}

object ArrayOffsetDateTimeColumn {
  def apply(values: Array[OffsetDateTime]) =
    new ArrayOffsetDateTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetDateTime]) =
    new ArrayOffsetDateTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetDateTimeColumn =
    new ArrayOffsetDateTimeColumn(new BitSet, new Array[OffsetDateTime](size))
}

class ArrayOffsetTimeColumn(val defined: BitSet, val values: Array[OffsetTime]) extends BitsetColumn(defined) with AccessibleArrayColumn[OffsetTime] with OffsetTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetTime) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[OffsetTime] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayOffsetTimeColumn(newDefined, newValues)
  }

}

object ArrayOffsetTimeColumn {
  def apply(values: Array[OffsetTime]) =
    new ArrayOffsetTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetTime]) =
    new ArrayOffsetTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetTimeColumn =
    new ArrayOffsetTimeColumn(new BitSet, new Array[OffsetTime](size))
}

class ArrayOffsetDateColumn(val defined: BitSet, val values: Array[OffsetDate]) extends BitsetColumn(defined) with AccessibleArrayColumn[OffsetDate] with OffsetDateColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: OffsetDate) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[OffsetDate] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayOffsetDateColumn(newDefined, newValues)
  }

}

object ArrayOffsetDateColumn {
  def apply(values: Array[OffsetDate]) =
    new ArrayOffsetDateColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[OffsetDate]) =
    new ArrayOffsetDateColumn(defined.copy, values)
  def empty(size: Int): ArrayOffsetDateColumn =
    new ArrayOffsetDateColumn(new BitSet, new Array[OffsetDate](size))
}

class ArrayLocalDateTimeColumn(val defined: BitSet, val values: Array[LocalDateTime]) extends BitsetColumn(defined) with AccessibleArrayColumn[LocalDateTime] with LocalDateTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalDateTime) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[LocalDateTime] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayLocalDateTimeColumn(newDefined, newValues)
  }

}

object ArrayLocalDateTimeColumn {
  def apply(values: Array[LocalDateTime]) =
    new ArrayLocalDateTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalDateTime]) =
    new ArrayLocalDateTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalDateTimeColumn =
    new ArrayLocalDateTimeColumn(new BitSet, new Array[LocalDateTime](size))
}

class ArrayLocalTimeColumn(val defined: BitSet, val values: Array[LocalTime]) extends BitsetColumn(defined) with AccessibleArrayColumn[LocalTime] with LocalTimeColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalTime) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[LocalTime] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayLocalTimeColumn(newDefined, newValues)
  }

}

object ArrayLocalTimeColumn {
  def apply(values: Array[LocalTime]) =
    new ArrayLocalTimeColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalTime]) =
    new ArrayLocalTimeColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalTimeColumn =
    new ArrayLocalTimeColumn(new BitSet, new Array[LocalTime](size))
}

class ArrayLocalDateColumn(val defined: BitSet, val values: Array[LocalDate]) extends BitsetColumn(defined) with AccessibleArrayColumn[LocalDate] with LocalDateColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: LocalDate) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[LocalDate] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayLocalDateColumn(newDefined, newValues)
  }

}

object ArrayLocalDateColumn {
  def apply(values: Array[LocalDate]) =
    new ArrayLocalDateColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[LocalDate]) =
    new ArrayLocalDateColumn(defined.copy, values)
  def empty(size: Int): ArrayLocalDateColumn =
    new ArrayLocalDateColumn(new BitSet, new Array[LocalDate](size))
}

class ArrayIntervalColumn(val defined: BitSet, val values: Array[DateTimeInterval]) extends BitsetColumn(defined) with AccessibleArrayColumn[DateTimeInterval] with IntervalColumn {
  def apply(row: Int) = values(row)

  def update(row: Int, value: DateTimeInterval) = {
    defined.set(row)
    values(row) = value
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[DateTimeInterval] = {
    val newValues = ArrayColumn.resizeArray(values, size)
    val newDefined = defined.resizeBits(size)
    new ArrayIntervalColumn(newDefined, newValues)
  }

}

object ArrayIntervalColumn {
  def apply(values: Array[DateTimeInterval]) =
    new ArrayIntervalColumn(BitSetUtil.range(0, values.length), values)
  def apply(defined: BitSet, values: Array[DateTimeInterval]) =
    new ArrayIntervalColumn(defined.copy, values)
  def empty(size: Int): ArrayIntervalColumn =
    new ArrayIntervalColumn(new BitSet, new Array[DateTimeInterval](size))
}

class MutableEmptyArrayColumn(val defined: BitSet) extends BitsetColumn(defined) with ArrayColumn[Boolean] with EmptyArrayColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Boolean] = {
    val newDefined = defined.resizeBits(size)
    new MutableEmptyArrayColumn(newDefined)
  }
}

object MutableEmptyArrayColumn {
  def empty(): MutableEmptyArrayColumn = new MutableEmptyArrayColumn(new BitSet)
}

class MutableEmptyObjectColumn(val defined: BitSet) extends BitsetColumn(defined) with ArrayColumn[Boolean] with EmptyObjectColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Boolean] = {
    val newDefined = defined.resizeBits(size)
    new MutableEmptyObjectColumn(newDefined)
  }
}

object MutableEmptyObjectColumn {
  def empty(): MutableEmptyObjectColumn = new MutableEmptyObjectColumn(new BitSet)
}

class MutableNullColumn(val defined: BitSet) extends BitsetColumn(defined) with ArrayColumn[Boolean] with NullColumn {
  def update(row: Int, value: Boolean) = {
    if (value) defined.set(row) else defined.clear(row)
    this
  }

  def clear(row: Int): Unit = defined.clear(row)

  def resize(size: Int): ArrayColumn[Boolean] = {
    val newDefined = defined.resizeBits(size)
    new MutableNullColumn(newDefined)
  }

}

object MutableNullColumn {
  def empty(): MutableNullColumn = new MutableNullColumn(new BitSet)
}

/* help for ctags
type ArrayColumn */

abstract class SingletonColumn[A] extends Column {

  override def isDefinedAt(row: Int) = row == 0

  override def definedAt(from: Int, to: Int): BitSet = SingletonColumn.Defined.copy

}

final class SingletonBoolColumn(val value: Boolean)
    extends SingletonColumn[Boolean] with BoolColumn {
  def apply(row: Int): Boolean = value
}

object SingletonBoolColumn {
  lazy val SingleTrue = new SingletonBoolColumn(true)
  lazy val SingleFalse = new SingletonBoolColumn(false)

  def bitset(b: Boolean) =
    if (b) SingletonColumn.BitSet_1
    else SingletonColumn.BitSet_0

  def apply(value: Boolean): SingletonBoolColumn =
    if (value) SingleTrue else SingleFalse
}

final case class SingletonLongColumn(value: Long)
    extends SingletonColumn[Long] with LongColumn {
  def apply(row: Int): Long = value
}

final case class SingletonDoubleColumn(value: Double)
    extends SingletonColumn[Double] with DoubleColumn {
  def apply(row: Int): Double = value
}

final case class SingletonNumColumn(value: BigDecimal)
    extends SingletonColumn[BigDecimal] with NumColumn {
  def apply(row: Int): BigDecimal = value
}

final case class SingletonStrColumn(value: String)
    extends SingletonColumn[String] with StrColumn {
  def apply(row: Int): String = value
}

final case class SingletonOffsetDateTimeColumn(value: OffsetDateTime)
    extends SingletonColumn[OffsetDateTime]with OffsetDateTimeColumn {
  def apply(row: Int): OffsetDateTime = value
}

final case class SingletonOffsetTimeColumn(value: OffsetTime)
    extends SingletonColumn[OffsetTime] with OffsetTimeColumn {
  def apply(row: Int): OffsetTime = value
}

final case class SingletonOffsetDateColumn(value: OffsetDate)
    extends SingletonColumn[OffsetDate] with OffsetDateColumn {
  def apply(row: Int): OffsetDate = value
}

final case class SingletonLocalDateTimeColumn(value: LocalDateTime)
    extends SingletonColumn[LocalDateTime] with LocalDateTimeColumn {
  def apply(row: Int): LocalDateTime = value
}

final case class SingletonLocalTimeColumn(value: LocalTime)
    extends SingletonColumn[LocalTime] with LocalTimeColumn {
  def apply(row: Int): LocalTime = value
}

final case class SingletonLocalDateColumn(value: LocalDate)
    extends SingletonColumn[LocalDate] with LocalDateColumn {
  def apply(row: Int): LocalDate = value
}

final case class SingletonIntervalColumn(value: DateTimeInterval)
    extends SingletonColumn[DateTimeInterval] with IntervalColumn {
  def apply(row: Int): DateTimeInterval = value
}

object SingletonColumn {
  val BitSet_0: BitSet = new BitSet

  val BitSet_1: BitSet = {
    val bs = new BitSet
    bs.set(0)
    bs
  }

  val Defined: BitSet = BitSet_1
}
