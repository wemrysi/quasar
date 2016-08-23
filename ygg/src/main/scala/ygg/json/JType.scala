package ygg.json

import ygg.common._

sealed trait JType {
  def |(jtype: JType): JType = JUnionT(this, jtype)
}

sealed trait JPrimitiveType extends JType
final case object JNumberT  extends JPrimitiveType
final case object JTextT    extends JPrimitiveType
final case object JBooleanT extends JPrimitiveType
final case object JNullT    extends JPrimitiveType
final case object JDateT    extends JPrimitiveType
final case object JPeriodT  extends JPrimitiveType

sealed trait JArrayT                                     extends JType
final case class JArrayHomogeneousT(jType: JType)        extends JArrayT
final case class JArrayFixedT(elements: Map[Int, JType]) extends JArrayT
final case object JArrayUnfixedT                         extends JArrayT

sealed trait JObjectT                                      extends JType
final case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
final case object JObjectUnfixedT                          extends JObjectT
final case class JUnionT(left: JType, right: JType)        extends JType

object JType {
  def Indexed(tps: (Int -> JType)*): JArrayFixedT    = JArrayFixedT(tps.toMap)
  def Object(tps: (String -> JType)*): JObjectFixedT = JObjectFixedT(tps.toMap)
  def Array(tps: JType*): JArrayFixedT               = JArrayFixedT(tps.zipWithIndex.map(x => x._2 -> x._1).toMap)
}
