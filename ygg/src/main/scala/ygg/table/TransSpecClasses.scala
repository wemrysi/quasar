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

import ygg._, common._, json._

package object trans {
  type TransSpec1 = TransSpec[Source1]
  type TransSpec2 = TransSpec[Source2]

  implicit class TransSpecOps[A <: SourceType](val spec: TransSpec[A]) {
    def inner_++(x: TransSpec[A], xs: TransSpec[A]*): InnerObjectConcat[A] = InnerObjectConcat(spec +: x +: xs: _*)
    def outer_++(x: TransSpec[A], xs: TransSpec[A]*): OuterObjectConcat[A] = OuterObjectConcat(spec +: x +: xs: _*)
  }

  implicit def transSpecBuilder[A <: SourceType](x: TransSpec[A]): TransSpecBuilder[A]       = new TransSpecBuilder(x)
  implicit def transSpecBuilderResult[A <: SourceType](x: TransSpecBuilder[A]): TransSpec[A] = x.spec
}

package trans {
  sealed trait SourceType       extends AnyRef
  sealed trait Source1          extends SourceType
  sealed trait Source2          extends SourceType
  final case object Source      extends Source1
  final case object SourceLeft  extends Source2
  final case object SourceRight extends Source2

  object root extends TransSpecBuilder(Leaf(Source)) {
    def value = selectDynamic("value")
    def key   = selectDynamic("key")
  }
  object rootLeft extends TransSpecBuilder(Leaf(SourceLeft)) {
    def value = selectDynamic("value")
    def key   = selectDynamic("key")
  }
  object rootRight extends TransSpecBuilder(Leaf(SourceRight)) {
    def value = selectDynamic("value")
    def key   = selectDynamic("key")
  }

  class TransSpecBuilder[A <: SourceType](val spec: TransSpec[A]) extends Dynamic {
    type This    = TransSpec[A]
    type Builder = TransSpecBuilder[A]

    protected def next[A <: SourceType](x: This): Builder = new TransSpecBuilder(x)

    def deepMap(pf: PartialFunction[This, This]): Builder = next(TransSpec.deepMap(spec)(pf))
    def deepMap1(fn: CF1): Builder                        = next(DeepMap1(spec, fn))
    def map1(fn: CF1): Builder                            = next(Map1(spec, fn))
    def isType(tp: JType): Builder                        = next(IsType(spec, tp))
    def apply(index: Int): Builder                        = next(DerefArrayStatic(spec, CPathIndex(index)))
    def delete(fields: CPathField*): Builder              = next(ObjectDelete(spec, fields.toSet))
    def select(field: CPathField): Builder                = next(DerefObjectStatic(spec, field))
    def select(name: String): Builder                     = select(CPathField(name))
    def selectDynamic(name: String): Builder              = select(name)
  }

  sealed trait TransSpec[+A <: SourceType]  extends AnyRef
  sealed trait ObjectSpec[+A <: SourceType] extends TransSpec[A]
  sealed trait ArraySpec[+A <: SourceType]  extends TransSpec[A]

  case class Leaf[+A <: SourceType](source: A) extends TransSpec[A] //done

  case class Filter[+A <: SourceType](source: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A] //done

  // Adds a column to the output in the manner of scanLeft
  case class Scan[+A <: SourceType](source: TransSpec[A], scanner: Scanner) extends TransSpec[A] //done

  case class Map1[+A <: SourceType](source: TransSpec[A], f: CF1) extends TransSpec[A] //done

  case class DeepMap1[+A <: SourceType](source: TransSpec[A], f: CF1) extends TransSpec[A] //done

  // apply a function to the cartesian product of the transformed left and right subsets of columns
  case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A], f: CF2) extends TransSpec[A] //done

  // Perform the specified transformation on the all sources, and then create a new set of columns
  // containing all the resulting columns.
  case class InnerObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends ObjectSpec[A] //done

  case class OuterObjectConcat[+A <: SourceType](objects: TransSpec[A]*) extends ObjectSpec[A] //done

  case class ObjectDelete[+A <: SourceType](source: TransSpec[A], fields: Set[CPathField]) extends TransSpec[A]

  case class InnerArrayConcat[+A <: SourceType](arrays: TransSpec[A]*) extends ArraySpec[A] //done

  case class OuterArrayConcat[+A <: SourceType](arrays: TransSpec[A]*) extends ArraySpec[A] //done

  // Take the output of the specified TransSpec and prefix all of the resulting selectors with the
  // specified field.
  case class WrapObject[+A <: SourceType](source: TransSpec[A], field: String) extends ObjectSpec[A] //done

  case class WrapObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

  case class WrapArray[+A <: SourceType](source: TransSpec[A]) extends ArraySpec[A] //done

  case class DerefObjectStatic[+A <: SourceType](source: TransSpec[A], field: CPathField) extends TransSpec[A] //done

  case class DerefMetadataStatic[+A <: SourceType](source: TransSpec[A], field: CPathMeta) extends TransSpec[A]

  case class DerefObjectDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done

  case class DerefArrayStatic[+A <: SourceType](source: TransSpec[A], element: CPathIndex) extends TransSpec[A] //done

  case class DerefArrayDynamic[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done

  case class ArraySwap[+A <: SourceType](source: TransSpec[A], index: Int) extends TransSpec[A]

  // Filter out all the source columns whose selector and CType are not specified by the supplied JType
  case class Typed[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A] // done

  // Filter out all the source columns whose selector and CType are not specified by the supplied JType
  // if the set of columns does not cover the JType specified, this will return the empty slice.
  case class TypedSubsumes[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A] // done

  // return a Boolean column
  // returns true for a given row when all of the columns specified by the supplied JType are defined
  case class IsType[+A <: SourceType](source: TransSpec[A], tpe: JType) extends TransSpec[A] // done

  case class Equal[+A <: SourceType](left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A] //done

  case class EqualLiteral[+A <: SourceType](left: TransSpec[A], right: CValue, invert: Boolean) extends TransSpec[A]

  // target is the transspec that provides defineedness information. The resulting table will be defined
  // and have the constant value wherever a row provided by the target transspec has at least one member
  // that is not undefined
  case class ConstLiteral[+A <: SourceType](value: CValue, target: TransSpec[A]) extends TransSpec[A]

  case class FilterDefined[+A <: SourceType](source: TransSpec[A], definedFor: TransSpec[A], definedness: Definedness) extends TransSpec[A]

  case class Cond[+A <: SourceType](pred: TransSpec[A], left: TransSpec[A], right: TransSpec[A]) extends TransSpec[A]

  object TransSpec {
    import CPath._

    def concatChildren[A <: SourceType](tree: CPathTree[Int], leaf: TransSpec[A] = Leaf(Source)): TransSpec[A] = {
      def createSpecs(trees: Seq[CPathTree[Int]]): Seq[TransSpec[A]] = trees map { child =>
        (child match {
          case node @ RootNode(seq)                  => concatChildren(node, leaf)
          case node @ FieldNode(CPathField(name), _) => trans.WrapObject(concatChildren(node, leaf), name)
          case node @ IndexNode(CPathIndex(_), _)    => trans.WrapArray(concatChildren(node, leaf)) //assuming that indices received in order
          case LeafNode(idx)                         => leaf(idx)
        }): TransSpec[A]
      }

      val initialSpecs = tree match {
        case RootNode(children)     => createSpecs(children)
        case FieldNode(_, children) => createSpecs(children)
        case IndexNode(_, children) => createSpecs(children)
        case LeafNode(_)            => Seq()
      }

      val result = initialSpecs reduceOption { (t1, t2) =>
        (t1, t2) match {
          case (t1: ObjectSpec[_], t2: ObjectSpec[_]) => trans.InnerObjectConcat(t1, t2)
          case (t1: ArraySpec[_], t2: ArraySpec[_])   => trans.InnerArrayConcat(t1, t2)
          case _                                      => abort("cannot have this")
        }
      }

      result getOrElse leaf
    }

    def mapSources[A <: SourceType, B <: SourceType](spec: TransSpec[A])(f: A => B): TransSpec[B] = spec match {
      case Leaf(source)                                   => Leaf(f(source))
      case ConstLiteral(value, target)                    => ConstLiteral(value, mapSources(target)(f))
      case Filter(source, pred)                           => Filter(mapSources(source)(f), mapSources(pred)(f))
      case FilterDefined(source, definedFor, definedness) => FilterDefined(mapSources(source)(f), mapSources(definedFor)(f), definedness)
      case Scan(source, scanner)                          => Scan(mapSources(source)(f), scanner)
      case Map1(source, f1)                               => Map1(mapSources(source)(f), f1)
      case DeepMap1(source, f1)                           => DeepMap1(mapSources(source)(f), f1)
      case Map2(left, right, f2)                          => Map2(mapSources(left)(f), mapSources(right)(f), f2)
      case OuterObjectConcat(objects @ _ *)               => OuterObjectConcat(objects.map(mapSources(_)(f)): _*)
      case InnerObjectConcat(objects @ _ *)               => InnerObjectConcat(objects.map(mapSources(_)(f)): _*)
      case ObjectDelete(source, fields)                   => ObjectDelete(mapSources(source)(f), fields)
      case InnerArrayConcat(arrays @ _ *)                 => InnerArrayConcat(arrays.map(mapSources(_)(f)): _*)
      case OuterArrayConcat(arrays @ _ *)                 => OuterArrayConcat(arrays.map(mapSources(_)(f)): _*)
      case WrapObject(source, field)                      => WrapObject(mapSources(source)(f), field)
      case WrapObjectDynamic(left, right)                 => WrapObjectDynamic(mapSources(left)(f), mapSources(right)(f))
      case WrapArray(source)                              => WrapArray(mapSources(source)(f))
      case DerefMetadataStatic(source, field)             => DerefMetadataStatic(mapSources(source)(f), field)
      case DerefObjectStatic(source, field)               => DerefObjectStatic(mapSources(source)(f), field)
      case DerefObjectDynamic(left, right)                => DerefObjectDynamic(mapSources(left)(f), mapSources(right)(f))
      case DerefArrayStatic(source, element)              => DerefArrayStatic(mapSources(source)(f), element)
      case DerefArrayDynamic(left, right)                 => DerefArrayDynamic(mapSources(left)(f), mapSources(right)(f))
      case ArraySwap(source, index)                       => ArraySwap(mapSources(source)(f), index)
      case Typed(source, tpe)                             => Typed(mapSources(source)(f), tpe)
      case TypedSubsumes(source, tpe)                     => TypedSubsumes(mapSources(source)(f), tpe)
      case IsType(source, tpe)                            => IsType(mapSources(source)(f), tpe)
      case Equal(left, right)                             => Equal(mapSources(left)(f), mapSources(right)(f))
      case EqualLiteral(source, value, invert)            => EqualLiteral(mapSources(source)(f), value, invert)
      case Cond(pred, left, right)                        => Cond(mapSources(pred)(f), mapSources(left)(f), mapSources(right)(f))
    }

    def deepMap[A <: SourceType](spec: TransSpec[A])(f: PartialFunction[TransSpec[A], TransSpec[A]]): TransSpec[A] = spec match {
      case x if f isDefinedAt x => f(x)

      case x @ Leaf(source)                  => x
      case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, deepMap(target)(f))

      case trans.Filter(source, pred) => trans.Filter(deepMap(source)(f), deepMap(pred)(f))
      case trans.FilterDefined(source, definedFor, definedness) =>
        trans.FilterDefined(deepMap(source)(f), deepMap(definedFor)(f), definedness)

      case Scan(source, scanner) => Scan(deepMap(source)(f), scanner)
      // case MapWith(source, mapper) => MapWith(deepMap(source)(f), mapper)

      case trans.Map1(source, f1)      => trans.Map1(deepMap(source)(f), f1)
      case trans.DeepMap1(source, f1)  => trans.DeepMap1(deepMap(source)(f), f1)
      case trans.Map2(left, right, f2) => trans.Map2(deepMap(left)(f), deepMap(right)(f), f2)

      case trans.OuterObjectConcat(objects @ _ *) => trans.OuterObjectConcat(objects.map(deepMap(_)(f)): _*)
      case trans.InnerObjectConcat(objects @ _ *) => trans.InnerObjectConcat(objects.map(deepMap(_)(f)): _*)
      case trans.ObjectDelete(source, fields)     => trans.ObjectDelete(deepMap(source)(f), fields)
      case trans.InnerArrayConcat(arrays @ _ *)   => trans.InnerArrayConcat(arrays.map(deepMap(_)(f)): _*)
      case trans.OuterArrayConcat(arrays @ _ *)   => trans.OuterArrayConcat(arrays.map(deepMap(_)(f)): _*)

      case trans.WrapObject(source, field)        => trans.WrapObject(deepMap(source)(f), field)
      case trans.WrapObjectDynamic(source, right) => trans.WrapObjectDynamic(deepMap(source)(f), deepMap(right)(f))
      case trans.WrapArray(source)                => trans.WrapArray(deepMap(source)(f))

      case DerefMetadataStatic(source, field) => DerefMetadataStatic(deepMap(source)(f), field)

      case DerefObjectStatic(source, field)  => deepMap(source)(f) select field
      case DerefObjectDynamic(left, right)   => DerefObjectDynamic(deepMap(left)(f), deepMap(right)(f))
      case DerefArrayStatic(source, element) => DerefArrayStatic(deepMap(source)(f), element)
      case DerefArrayDynamic(left, right)    => DerefArrayDynamic(deepMap(left)(f), deepMap(right)(f))

      case trans.ArraySwap(source, index) => trans.ArraySwap(deepMap(source)(f), index)

      case Typed(source, tpe)         => Typed(deepMap(source)(f), tpe)
      case TypedSubsumes(source, tpe) => TypedSubsumes(deepMap(source)(f), tpe)
      case IsType(source, tpe)        => IsType(deepMap(source)(f), tpe)

      case trans.Equal(left, right)                  => trans.Equal(deepMap(left)(f), deepMap(right)(f))
      case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(deepMap(source)(f), value, invert)

      case trans.Cond(pred, left, right) => trans.Cond(deepMap(pred)(f), deepMap(left)(f), deepMap(right)(f))
    }
  }

  object TransSpec1 {
    import constants._

    val Id              = root.spec
    val DerefArray0     = root(0)
    val DerefArray1     = root(1)
    val DerefArray2     = root(2)
    val PruneToKeyValue = WrapObject(SourceKey.Single, Key.name) inner_++ WrapObject(SourceValue.Single, Value.name)
    val DeleteKeyValue  = Id.delete(Key, Value)
  }

  object TransSpec2 {
    import constants._

    val LeftId = rootLeft.spec

    /** Flips all `SourceLeft`s to `SourceRight`s and vice versa. */
    def flip(spec: TransSpec2): TransSpec2 = TransSpec.mapSources(spec) {
      case SourceLeft  => SourceRight
      case SourceRight => SourceLeft
    }

    def DerefArray0(source: Source2) = root(0)
    def DerefArray1(source: Source2) = root(1)
    def DerefArray2(source: Source2) = root(2)

    val DeleteKeyValueLeft  = Leaf(SourceLeft).delete(Key, Value)
    val DeleteKeyValueRight = Leaf(SourceRight).delete(Key, Value)
  }

  sealed trait GroupKeySpec {
    def &&(rhs: GroupKeySpec): GroupKeySpec = GroupKeySpecAnd(this, rhs)
    def ||(rhs: GroupKeySpec): GroupKeySpec = GroupKeySpecOr(this, rhs)
  }

  /**
    * Definition for a single (non-composite) key part.
    *
    * @param key The key which will be used by `merge` to access this particular tic-variable (which may be refined by more than one `GroupKeySpecSource`)
    * @param spec A transform which defines this key part as a function of the source table in `GroupingSource`.
    */
  case class GroupKeySpecSource(key: CPathField, spec: TransSpec1)    extends GroupKeySpec
  case class GroupKeySpecAnd(left: GroupKeySpec, right: GroupKeySpec) extends GroupKeySpec
  case class GroupKeySpecOr(left: GroupKeySpec, right: GroupKeySpec)  extends GroupKeySpec

  object GroupKeySpec {
    def dnf(keySpec: GroupKeySpec): GroupKeySpec = {
      keySpec match {
        case GroupKeySpecSource(key, spec)                  => GroupKeySpecSource(key, spec)
        case GroupKeySpecAnd(GroupKeySpecOr(ol, or), right) => GroupKeySpecOr(dnf(GroupKeySpecAnd(ol, right)), dnf(GroupKeySpecAnd(or, right)))
        case GroupKeySpecAnd(left, GroupKeySpecOr(ol, or))  => GroupKeySpecOr(dnf(GroupKeySpecAnd(left, ol)), dnf(GroupKeySpecAnd(left, or)))

        case gand @ GroupKeySpecAnd(left, right) =>
          val leftdnf  = dnf(left)
          val rightdnf = dnf(right)
          if (leftdnf == left && rightdnf == right) gand else dnf(GroupKeySpecAnd(leftdnf, rightdnf))

        case gor @ GroupKeySpecOr(left, right) =>
          val leftdnf  = dnf(left)
          val rightdnf = dnf(right)
          if (leftdnf == left && rightdnf == right) gor else dnf(GroupKeySpecOr(leftdnf, rightdnf))
      }
    }

    def toVector(keySpec: GroupKeySpec): Vector[GroupKeySpec] = {
      keySpec match {
        case GroupKeySpecOr(left, right) => toVector(left) ++ toVector(right)
        case x                           => Vector(x)
      }
    }
  }

  object constants {
    val Key     = CPathField("key")
    val Value   = CPathField("value")
    val Group   = CPathField("group")
    val SortKey = CPathField("sortkey")

    object SourceKey {
      val Single = root.key
      val Left   = rootLeft.key
      val Right  = rootRight.key
    }
    object SourceValue {
      val Single = root.value
      val Left   = rootLeft.value
      val Right  = rootRight.value
    }
  }
}
