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

/** Stub for a de-caked version. Not in use yet.
 */

/**

import ygg._, common._, json._

trait TransformClasses[A <: trans.SourceType] {
  sealed trait TransSpec  extends AnyRef
  sealed trait ObjectSpec extends TransSpec
  sealed trait ArraySpec  extends TransSpec

  final case class InnerObjectConcat(objects: TransSpec*)       extends ObjectSpec //done
  final case class OuterObjectConcat(objects: TransSpec*)       extends ObjectSpec //done
  final case class WrapObject(source: TransSpec, field: String) extends ObjectSpec //done

  final case class InnerArrayConcat(arrays: TransSpec*) extends ArraySpec //done
  final case class OuterArrayConcat(arrays: TransSpec*) extends ArraySpec //done
  final case class WrapArray(source: TransSpec)         extends ArraySpec //done

  final case class DeepMap1(source: TransSpec, f: CF1)                      extends TransSpec //done
  final case class DerefArrayDynamic(left: TransSpec, right: TransSpec)     extends TransSpec //done
  final case class DerefArrayStatic(source: TransSpec, element: CPathIndex) extends TransSpec //done
  final case class DerefObjectDynamic(left: TransSpec, right: TransSpec)    extends TransSpec //done
  final case class DerefObjectStatic(source: TransSpec, field: CPathField)  extends TransSpec //done
  final case class Equal(left: TransSpec, right: TransSpec)                 extends TransSpec //done
  final case class Filter(source: TransSpec, predicate: TransSpec)          extends TransSpec //done
  final case class IsType(source: TransSpec, tpe: JType)                    extends TransSpec //done
  final case class Leaf(source: A)                                          extends TransSpec //done
  final case class Map1(source: TransSpec, f: CF1)                          extends TransSpec //done
  final case class Map2(left: TransSpec, right: TransSpec, f: CF2)          extends TransSpec //done
  final case class Scan(source: TransSpec, scanner: Scanner)                extends TransSpec //done
  final case class TypedSubsumes(source: TransSpec, tpe: JType)             extends TransSpec //done
  final case class Typed(source: TransSpec, tpe: JType)                     extends TransSpec //done

  final case class ArraySwap(source: TransSpec, index: Int)                                          extends TransSpec
  final case class Cond(pred: TransSpec, left: TransSpec, right: TransSpec)                          extends TransSpec
  final case class ConstLiteral(value: CValue, target: TransSpec)                                    extends TransSpec
  final case class DerefMetadataStatic(source: TransSpec, field: CPathMeta)                          extends TransSpec
  final case class EqualLiteral(left: TransSpec, right: CValue, invert: Boolean)                     extends TransSpec
  final case class FilterDefined(source: TransSpec, definedFor: TransSpec, definedness: Definedness) extends TransSpec
  final case class ObjectDelete(source: TransSpec, fields: Set[CPathField])                          extends TransSpec
  final case class WrapObjectDynamic(left: TransSpec, right: TransSpec)                              extends TransSpec
}
**/
