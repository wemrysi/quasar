package ygg.table

import scalaz.{ =?> => _, _ }, Ordering._
import ygg._, common._, json._, data._

trait Column {
  val tpe: CType

  def isDefinedAt(row: Int): Boolean
  def jValue(row: Int): JValue
  def cValue(row: Int): CValue
  def strValue(row: Int): String
  def rowCompare(row1: Int, row2: Int): Int

  def |>(f1: CF1): Option[Column]           = f1(this)
  def toString(row: Int): String            = if (isDefinedAt(row)) strValue(row) else "(undefined)"
  def toString(range: Range): String        = range map (this toString _) mkString ("(", ",", ")")
  def definedAt(from: Int, to: Int): BitSet = Bits.filteredRange(from, to)(isDefinedAt)
}

object Table

trait Table {
  def slices: StreamT[Need, Slice]

  /**
    * Return an indication of table size, if known
    */
  def size: TableSize

  /**
    * For each distinct path in the table, load all columns identified by the specified
    * jtype and concatenate the resulting slices into a new table.
    */
  def load(apiKey: APIKey, tpe: JType): Need[Table]

  /**
    * Folds over the table to produce a single value (stored in a singleton table).
    */
  def reduce[A: Monoid](reducer: CReducer[A]): Need[A]

  /**
    * Removes all rows in the table for which definedness is satisfied
    * Remaps the indicies.
    */
  // def compact(spec: TransSpec1, definedness: Definedness = AnyDefined): Table

  /**
    * Performs a one-pass transformation of the keys and values in the table.
    * If the key transform is not identity, the resulting table will have
    * unknown sort order.
    */
  // def transform(spec: TransSpec1): Table

  /**
    * Cogroups this table with another table, using equality on the specified
    * transformation on rows of the table.
    */
  // def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table

  /**
    * Performs a full cartesian cross on this table with the specified table,
    * applying the specified transformation to merge the two tables into
    * a single table.
    */
  // def cross(that: Table)(spec: TransSpec2): Table

  /**
    * Force the table to a backing store, and provice a restartable table
    * over the results.
    */
  def force: Need[Table]

  def paged(limit: Int): Table

  /**
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param sortKey The transspec to use to obtain the values to sort on
    * @param sortOrder Whether to sort ascending or descending
    * @param unique If true, the same key values will sort into a single row, otherwise
    * we assign a unique row ID as part of the key so that multiple equal values are
    * preserved
    */
  // def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Table]

  // def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): Need[Table] = sort(sortKey, sortOrder, unique = false)
  // def sort(sortKey: TransSpec1): Need[Table]                              = sort(sortKey, SortAscending)

  // def distinct(spec: TransSpec1): Table
  // def concat(t2: Table): Table
  // def zip(t2: Table): Need[Table]
  def toArray[A](implicit tpe: CValueType[A]): Table

  /**
    * Sorts the KV table by ascending or descending order based on a seq of transformations
    * applied to the rows.
    *
    * @param groupKeys The transspecs to use to obtain the values to sort on
    * @param valueSpec The transspec to use to obtain the non-sorting values
    * @param sortOrder Whether to sort ascending or descending
    * @param unique If true, the same key values will sort into a single row, otherwise
    * we assign a unique row ID as part of the key so that multiple equal values are
    * preserved
    */
  // def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Seq[Table]]
  // def partitionMerge(partitionBy: TransSpec1)(f: Table => Need[Table]): Need[Table]
  def takeRange(startIndex: Long, numberToTake: Long): Table
  def canonicalize(length: Int): Table
  def schemas: Need[Set[JType]]
  def renderJson(prefix: String = "", delimiter: String = "\n", suffix: String = ""): StreamT[Need, CharBuffer]

  // for debugging only!!
  def toJson: Need[Stream[JValue]]
}
trait TableCompanion {
  def empty: Table
  def constString(v: scSet[String]): Table
  def constLong(v: scSet[Long]): Table
  def constDouble(v: scSet[Double]): Table
  def constDecimal(v: scSet[BigDecimal]): Table
  def constDate(v: scSet[DateTime]): Table
  def constBoolean(v: scSet[Boolean]): Table
  def constNull: Table
  def constEmptyObject: Table
  def constEmptyArray: Table
  def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): Table
}


trait HomogeneousArrayColumn[@spec(Boolean, Long, Double) A] extends Column with (Int => Array[A]) { self =>
  val tpe: CArrayType[A]
  def apply(row: Int): Array[A]
  def isDefinedAt(row: Int): Boolean

  def rowCompare(row1: Int, row2: Int): Int = abort("...")

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
  def apply[A: CValueType](f: Int =?> Array[A]): HomogeneousArrayColumn[A] = new HomogeneousArrayColumn[A] {
    val tpe: CArrayType[A]    = implicitly
    def isDefinedAt(row: Int) = f isDefinedAt row
    def apply(row: Int)       = f(row)
  }
  def unapply[A](col: HomogeneousArrayColumn[A]): Option[CValueType[A]] = Some(col.tpe.elemType)

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
    case col @ HomogeneousArrayColumn(CDate) =>
      new DateColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): DateTime = col(row)(i)
      }
    case col @ HomogeneousArrayColumn(CPeriod) =>
      new PeriodColumn {
        def isDefinedAt(row: Int): Boolean =
          i >= 0 && col.isDefinedAt(row) && i < col(row).length
        def apply(row: Int): Period = col(row)(i)
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
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  def asBitSet(undefinedVal: Boolean, size: Int): BitSet = {
    val back = new BitSet(size)
    var i    = 0
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

  override val tpe                        = CBoolean
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
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe                        = CLong
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CLong(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString                   = "LongColumn"
}

trait DoubleColumn extends Column with (Int => Double) {
  def apply(row: Int): Double
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe                        = CDouble
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CDouble(this(row))
  override def strValue(row: Int): String = String.valueOf(this(row))
  override def toString                   = "DoubleColumn"
}

trait NumColumn extends Column with (Int => BigDecimal) {
  def apply(row: Int): BigDecimal
  def rowCompare(row1: Int, row2: Int): Int = apply(row1) compare apply(row2)

  override val tpe                        = CNum
  override def jValue(row: Int)           = JNum(this(row))
  override def cValue(row: Int)           = CNum(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "NumColumn"
}

trait StrColumn extends Column with (Int => String) {
  def apply(row: Int): String
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compareTo apply(row2)

  override val tpe                        = CString
  override def jValue(row: Int)           = JString(this(row))
  override def cValue(row: Int)           = CString(this(row))
  override def strValue(row: Int): String = this(row)
  override def toString                   = "StrColumn"
}

trait DateColumn extends Column with (Int => DateTime) {
  def apply(row: Int): DateTime
  def rowCompare(row1: Int, row2: Int): Int =
    apply(row1) compareTo apply(row2)

  override val tpe                        = CDate
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CDate(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "DateColumn"
}

trait PeriodColumn extends Column with (Int => Period) {
  def apply(row: Int): Period
  def rowCompare(row1: Int, row2: Int): Int = abort("Cannot compare periods.")

  override val tpe                        = CPeriod
  override def jValue(row: Int)           = JString(this(row).toString)
  override def cValue(row: Int)           = CPeriod(this(row))
  override def strValue(row: Int): String = this(row).toString
  override def toString                   = "PeriodColumn"
}

trait EmptyArrayColumn extends Column {
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe                          = CEmptyArray
  override def jValue(row: Int)             = jarray()
  override def cValue(row: Int)             = CEmptyArray
  override def strValue(row: Int): String   = "[]"
  override def toString                     = "EmptyArrayColumn"
}
object EmptyArrayColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyArrayColumn
}

trait EmptyObjectColumn extends Column {
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe                          = CEmptyObject
  override def jValue(row: Int)             = jobject()
  override def cValue(row: Int)             = CEmptyObject
  override def strValue(row: Int): String   = "{}"
  override def toString                     = "EmptyObjectColumn"
}

object EmptyObjectColumn {
  def apply(definedAt: BitSet) = new BitsetColumn(definedAt) with EmptyObjectColumn
}

trait NullColumn extends Column {
  def rowCompare(row1: Int, row2: Int): Int = 0
  override val tpe                          = CNull
  override def jValue(row: Int)             = JNull
  override def cValue(row: Int)             = CNull
  override def strValue(row: Int): String   = "null"
  override def toString                     = "NullColumn"
}
object NullColumn {
  def apply(definedAt: BitSet) = {
    new BitsetColumn(definedAt) with NullColumn
  }
}

object UndefinedColumn {
  private def fail() = abort("Values in undefined columns SHOULD NOT BE ACCESSED")

  def apply(col: Column) = new Column {
    val tpe                                   = col.tpe
    def rowCompare(row1: Int, row2: Int): Int = abort("Cannot compare undefined values.")
    def isDefinedAt(row: Int)                 = false
    def jValue(row: Int)                      = fail()
    def cValue(row: Int)                      = CUndefined
    def strValue(row: Int)                    = fail()
  }

  val raw = new Column {
    val tpe                                   = CUndefined
    def rowCompare(row1: Int, row2: Int): Int = abort("Cannot compare undefined values.")
    def isDefinedAt(row: Int)                 = false
    def jValue(row: Int)                      = fail()
    def cValue(row: Int)                      = CUndefined
    def strValue(row: Int)                    = fail()
  }
}

object Column {
  def rowOrder(col: Column): Ord[Int] = Ord.order[Int] {
    case (i, j) if (col isDefinedAt i) && (col isDefinedAt j) => Cmp(col.rowCompare(i, j))
    case (i, _) if (col isDefinedAt i)                        => GT
    case (_, j) if (col isDefinedAt j)                        => LT
    case _                                                    => EQ
  }

  @inline def const(cv: CValue): Column = cv match {
    case CBoolean(v)                         => const(v)
    case CLong(v)                            => const(v)
    case CDouble(v)                          => const(v)
    case CNum(v)                             => const(v)
    case CString(v)                          => const(v)
    case CDate(v)                            => const(v)
    case CArray(v, t @ CArrayType(elemType)) => const(v)(elemType)
    case CEmptyObject                        => new InfiniteColumn with EmptyObjectColumn
    case CEmptyArray                         => new InfiniteColumn with EmptyArrayColumn
    case CNull                               => new InfiniteColumn with NullColumn
    case CUndefined                          => UndefinedColumn.raw
    case _                                   => abort(s"Unexpected arg $cv")
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

  @inline def const(v: DateTime) = new InfiniteColumn with DateColumn {
    def apply(row: Int) = v
  }

  @inline def const(v: Period) = new InfiniteColumn with PeriodColumn {
    def apply(row: Int) = v
  }

  @inline def const[@spec(Boolean, Long, Double) A: CValueType](v: Array[A]) = new InfiniteColumn with HomogeneousArrayColumn[A] {
    val tpe             = CArrayType(CValueType[A])
    def apply(row: Int) = v
  }

  object unionRightSemigroup extends Semigroup[Column] {
    def append(c1: Column, c2: => Column): Column = {
      cf.UnionRight(c1, c2) getOrElse {
        abort("Illgal attempt to merge columns of dissimilar type: " + c1.tpe + "," + c2.tpe)
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
