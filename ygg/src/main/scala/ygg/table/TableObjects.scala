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

sealed trait GroupingSpec

/**
  * Definition for a single group set and its associated composite key part.
  *
  * @param table The target set for the grouping
  * @param targetTrans The key which will be used by `merge` to access a particular subset of the target
  * @param groupKeySpec A composite union/intersect overlay on top of transspec indicating the composite key for this target set
  */
final case class GroupingSource(table: Table, idTrans: TransSpec1, targetTrans: Option[TransSpec1], groupId: GroupId, groupKeySpec: GroupKeySpec)
    extends GroupingSpec

final case class GroupingAlignment(groupKeyLeftTrans: TransSpec1,
                             groupKeyRightTrans: TransSpec1,
                             left: GroupingSpec,
                             right: GroupingSpec,
                             alignment: GroupingSpec.Alignment)
    extends GroupingSpec
