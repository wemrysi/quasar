package ygg.table

import ygg.json._

/** Stub for a de-caked version. Not in use yet.
 */
trait TransformClasses[A <: trans.SourceType] {
  sealed trait TransSpec  extends AnyRef
  sealed trait ObjectSpec extends TransSpec
  sealed trait ArraySpec  extends TransSpec

  case class InnerObjectConcat(objects: TransSpec*)       extends ObjectSpec //done
  case class OuterObjectConcat(objects: TransSpec*)       extends ObjectSpec //done
  case class WrapObject(source: TransSpec, field: String) extends ObjectSpec //done

  case class InnerArrayConcat(arrays: TransSpec*) extends ArraySpec //done
  case class OuterArrayConcat(arrays: TransSpec*) extends ArraySpec //done
  case class WrapArray(source: TransSpec)         extends ArraySpec //done

  case class DeepMap1(source: TransSpec, f: CF1)                      extends TransSpec //done
  case class DerefArrayDynamic(left: TransSpec, right: TransSpec)     extends TransSpec //done
  case class DerefArrayStatic(source: TransSpec, element: CPathIndex) extends TransSpec //done
  case class DerefObjectDynamic(left: TransSpec, right: TransSpec)    extends TransSpec //done
  case class DerefObjectStatic(source: TransSpec, field: CPathField)  extends TransSpec //done
  case class Equal(left: TransSpec, right: TransSpec)                 extends TransSpec //done
  case class Filter(source: TransSpec, predicate: TransSpec)          extends TransSpec //done
  case class IsType(source: TransSpec, tpe: JType)                    extends TransSpec //done
  case class Leaf(source: A)                                          extends TransSpec //done
  case class Map1(source: TransSpec, f: CF1)                          extends TransSpec //done
  case class Map2(left: TransSpec, right: TransSpec, f: CF2)          extends TransSpec //done
  case class Scan(source: TransSpec, scanner: Scanner)                extends TransSpec //done
  case class TypedSubsumes(source: TransSpec, tpe: JType)             extends TransSpec //done
  case class Typed(source: TransSpec, tpe: JType)                     extends TransSpec //done

  case class ArraySwap(source: TransSpec, index: Int)                                          extends TransSpec
  case class Cond(pred: TransSpec, left: TransSpec, right: TransSpec)                          extends TransSpec
  case class ConstLiteral(value: CValue, target: TransSpec)                                    extends TransSpec
  case class DerefMetadataStatic(source: TransSpec, field: CPathMeta)                          extends TransSpec
  case class EqualLiteral(left: TransSpec, right: CValue, invert: Boolean)                     extends TransSpec
  case class FilterDefined(source: TransSpec, definedFor: TransSpec, definedness: Definedness) extends TransSpec
  case class ObjectDelete(source: TransSpec, fields: Set[CPathField])                          extends TransSpec
  case class WrapObjectDynamic(left: TransSpec, right: TransSpec)                              extends TransSpec
}
