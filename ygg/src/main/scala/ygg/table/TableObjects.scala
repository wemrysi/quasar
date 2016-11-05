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

import ygg._, common._, trans._

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

sealed trait GroupingSpec[T] {
  val rep: TableRep[T]
}
final case class GroupingSource[T](
  rep: TableRep[T],
  table: T,
  idTrans: TransSpec1,
  targetTrans: Option[TransSpec1],
  groupId: GroupId,
  groupKeySpec: GroupKeySpec
) extends GroupingSpec[T]

final case class GroupingAlignment[T](
  rep: TableRep[T],
  groupKeyLeftTrans: TransSpec1,
  groupKeyRightTrans: TransSpec1,
  left: GroupingSpec[T],
  right: GroupingSpec[T],
  alignment: GroupingSpec.Alignment
) extends GroupingSpec[T]

object GroupingAlignment {
  def intersect[T](l: GroupingSpec[T], r: GroupingSpec[T]): GroupingAlignment[T] =
    GroupingAlignment(l.rep, TransSpec1.Id, TransSpec1.Id, l, r, GroupingSpec.Intersection)
}

sealed trait TableSize {
  def maxSize: Long
  def +(other: TableSize): TableSize
  def *(other: TableSize): TableSize

  def isAtLeast(minSize: Long): TableSize = this match {
    case EstimateSize(min, max) if min < minSize => EstimateSize(minSize, max)
    case _                                       => this
  }
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
  case object CrossLeft        extends CrossOrder
  case object CrossRight       extends CrossOrder
  case object CrossLeftRight   extends CrossOrder
  case object CrossRightLeft   extends CrossOrder
}
