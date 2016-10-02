/*
 * Copyright 2014–2016 SlamData Inc.
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

package ygg.table

import ygg._, common._, data._, trans._

object F1Expr {
  def negate         = cf.math.Negate
  def coerceToDouble = cf.CoerceToDouble
  def moduloTwo      = cf.math.Mod applyr CLong(2)
  def equalsZero     = cf.std.Eq applyr CLong(0)
  def isEven         = moduloTwo andThen equalsZero
}
object Fn {
  def source: TransSpec1        = root
  def valueIsEven(name: String) = root select name map1 F1Expr.isEven
  def constantTrue              = Filter(source, trans.Equal(source, source))
}
trait CF1Like {
  def compose(f1: CF1): CF1
  def andThen(f1: CF1): CF1
}
trait CF2Like {
  def applyl(cv: CValue): CF1
  def applyr(cv: CValue): CF1
  def andThen(f1: CF1): CF2
}

sealed trait Definedness extends Product with Serializable
case object AnyDefined extends Definedness
case object AllDefined extends Definedness

object GroupingSpec {
  sealed trait Alignment extends Product with Serializable
  case object Union        extends Alignment
  case object Intersection extends Alignment
}

/**
  * Definition for a single group set and its associated composite key part.
  *
  * @param table The target set for the grouping
  * @param targetTrans The key which will be used by `merge` to access a particular subset of the target
  * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
  */

sealed trait GroupingSpec
final case class GroupingSource(
  table: Table,
  idTrans: TransSpec1,
  targetTrans: Option[TransSpec1],
  groupId: GroupId,
  groupKeySpec: GroupKeySpec
) extends GroupingSpec

final case class GroupingAlignment(
  groupKeyLeftTrans: TransSpec1,
  groupKeyRightTrans: TransSpec1,
  left: GroupingSpec,
  right: GroupingSpec,
  alignment: GroupingSpec.Alignment
) extends GroupingSpec

final case class APIKey(value: String) extends AnyVal

sealed trait TableSize {
  def maxSize: Long
  def +(other: TableSize): TableSize
  def *(other: TableSize): TableSize
}
object TableSize {
  def apply(size: Int): TableSize            = apply(size.toLong)
  def apply(size: Long): TableSize           = ExactSize(size)
  def apply(min: Long, max: Long): TableSize = if (min != max) EstimateSize(min, max) else ExactSize(min)
}
object ExactSize {
  def apply(min: Int): ExactSize = new ExactSize(min.toLong)
}
final case class ExactSize(minSize: Long) extends TableSize {
  val maxSize = minSize

  def +(other: TableSize) = other match {
    case ExactSize(n)         => ExactSize(minSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, minSize + n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }

  def *(other: TableSize) = other match {
    case ExactSize(n)         => ExactSize(minSize * n)
    case EstimateSize(n1, n2) => EstimateSize(minSize * n1, minSize * n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }
}
final case class EstimateSize(minSize: Long, maxSize: Long) extends TableSize {
  def +(other: TableSize) = other match {
    case ExactSize(n)         => EstimateSize(minSize + n, maxSize + n)
    case EstimateSize(n1, n2) => EstimateSize(minSize + n1, maxSize + n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }

  def *(other: TableSize) = other match {
    case ExactSize(n)         => EstimateSize(minSize * n, maxSize * n)
    case EstimateSize(n1, n2) => EstimateSize(minSize * n1, maxSize * n2)
    case UnknownSize          => UnknownSize
    case InfiniteSize         => InfiniteSize
  }
}
final case object UnknownSize extends TableSize {
  val maxSize             = Long.MaxValue
  def +(other: TableSize) = UnknownSize
  def *(other: TableSize) = UnknownSize
}
final case object InfiniteSize extends TableSize {
  val maxSize             = Long.MaxValue
  def +(other: TableSize) = InfiniteSize
  def *(other: TableSize) = InfiniteSize
}


sealed trait SortOrder           extends Product with Serializable
sealed trait DesiredSortOrder    extends SortOrder { def isAscending: Boolean }
final case object SortAscending  extends DesiredSortOrder { val isAscending = true  }
final case object SortDescending extends DesiredSortOrder { val isAscending = false }

sealed trait JoinOrder extends Product with Serializable
final object JoinOrder {
  case object LeftOrder  extends JoinOrder
  case object RightOrder extends JoinOrder
  case object KeyOrder   extends JoinOrder
}

sealed trait CrossOrder extends Product with Serializable
final object CrossOrder {
  case object CrossLeft      extends CrossOrder
  case object CrossRight     extends CrossOrder
  case object CrossLeftRight extends CrossOrder
  case object CrossRightLeft extends CrossOrder
}

object aligns {
  sealed trait AlignState extends Product with Serializable
  final case class RunLeft(rightRow: Int, rightKey: Slice, rightAuthority: Option[Slice]) extends AlignState
  final case class RunRight(leftRow: Int, leftKey: Slice, rightAuthority: Option[Slice])  extends AlignState
  final case class FindEqualAdvancingRight(leftRow: Int, leftKey: Slice)                  extends AlignState
  final case class FindEqualAdvancingLeft(rightRow: Int, rightKey: Slice)                 extends AlignState

  sealed trait Span extends Product with Serializable
  final case object LeftSpan  extends Span
  final case object RightSpan extends Span
  final case object NoSpan    extends Span

  sealed trait NextStep extends Product with Serializable
  final case class MoreLeft(span: Span, leq: BitSet, ridx: Int, req: BitSet)  extends NextStep
  final case class MoreRight(span: Span, lidx: Int, leq: BitSet, req: BitSet) extends NextStep
}

object JDBM {
  type Bytes             = Array[Byte]
  type BtoBEntry         = jMapEntry[Bytes, Bytes]
  type BtoBIterator      = scIterator[BtoBEntry]
  type BtoBMap           = java.util.SortedMap[Bytes, Bytes]
  type BtoBConcurrentMap = jConcurrentMap[Bytes, Bytes]

  final case class JSlice(firstKey: Bytes, lastKey: Bytes, rows: Int)

  type IndexStore = BtoBConcurrentMap
  type IndexMap   = Map[IndexKey, SliceSorter]

  sealed trait SliceSorter {
    def name: String
    def keyRefs: Array[ColumnRef]
    def valRefs: Array[ColumnRef]
    def count: Long
  }

  final case class SliceIndex(name: String,
                        dbFile: jFile,
                        storage: IndexStore,
                        keyRowFormat: RowFormat,
                        keyComparator: Comparator[Bytes],
                        keyRefs: Array[ColumnRef],
                        valRefs: Array[ColumnRef],
                        count: Long) extends SliceSorter

  final case class SortedSlice(name: String,
                         kslice: Slice,
                         vslice: Slice,
                         valEncoder: ColumnEncoder,
                         keyRefs: Array[ColumnRef],
                         valRefs: Array[ColumnRef],
                         count: Long) extends SliceSorter

  final case class IndexKey(streamId: String, keyRefs: List[ColumnRef], valRefs: List[ColumnRef]) {
    val name = streamId + ";krefs=" + keyRefs.mkString("[", ",", "]") + ";vrefs=" + valRefs.mkString("[", ",", "]")
  }

  def columnFor(prefix: CPath, sliceSize: Int)(ref: ColumnRef): ColumnRef -> ArrayColumn[_] = ((
     ref.copy(selector = prefix \ ref.selector),
     ref.ctype match {
       case CString              => ArrayStrColumn.empty(sliceSize)
       case CBoolean             => ArrayBoolColumn.empty()
       case CLong                => ArrayLongColumn.empty(sliceSize)
       case CDouble              => ArrayDoubleColumn.empty(sliceSize)
       case CNum                 => ArrayNumColumn.empty(sliceSize)
       case CDate                => ArrayDateColumn.empty(sliceSize)
       case CPeriod              => ArrayPeriodColumn.empty(sliceSize)
       case CNull                => MutableNullColumn.empty()
       case CEmptyObject         => MutableEmptyObjectColumn.empty()
       case CEmptyArray          => MutableEmptyArrayColumn.empty()
       case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(sliceSize)(elemType)
       case CUndefined           => abort("CUndefined cannot be serialized")
     }
  ))
}
