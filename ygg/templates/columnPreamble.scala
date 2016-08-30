package ygg.boil

import scalaz._, Scalaz._
import ygg._, common._, json._

trait Column {
  val tpe: CType

  def isDefinedAt(row: ${{ROWID}}): Boolean
  def jValue(row: ${{ROWID}}): JValue
  def cValue(row: ${{ROWID}}): CValue
  def strValue(row: ${{ROWID}}): String
  def rowCompare(row1: ${{ROWID}}, row2: ${{ROWID}}): Ordering
}

sealed trait CType
sealed trait CValue
sealed trait CWrappedValue[A] extends CValue

final case class CArrayType[A](elem: CType) extends CType
final case class CArray[A](as: Array[A], tp: CArrayType[A]) extends CWrappedValue[Array[A]]

final object CArray {
  def apply[A](as: Array[A])(implicit elem: CType): CArray[A] =
    CArray[A](as, CArrayType[A](elem))
}
