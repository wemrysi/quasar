package ygg.table

import ygg._
import trans._

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

sealed trait Definedness
case object AnyDefined extends Definedness
case object AllDefined extends Definedness

object GroupingSpec {
  sealed trait Alignment
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


sealed trait SortOrder           extends AnyRef
sealed trait DesiredSortOrder    extends SortOrder { def isAscending: Boolean }
final case object SortAscending  extends DesiredSortOrder { val isAscending = true  }
final case object SortDescending extends DesiredSortOrder { val isAscending = false }

sealed trait JoinOrder
final object JoinOrder {
  case object LeftOrder  extends JoinOrder
  case object RightOrder extends JoinOrder
  case object KeyOrder   extends JoinOrder
}

sealed trait CrossOrder
final object CrossOrder {
  case object CrossLeft      extends CrossOrder
  case object CrossRight     extends CrossOrder
  case object CrossLeftRight extends CrossOrder
  case object CrossRightLeft extends CrossOrder
}
