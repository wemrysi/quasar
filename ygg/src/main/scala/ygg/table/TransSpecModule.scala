package ygg.table

import ygg.common._
import ygg.json._
import TransSpecModule._

object TransSpecModule {
  object paths {
    val Key     = CPathField("key")
    val Value   = CPathField("value")
    val Group   = CPathField("group")
    val SortKey = CPathField("sortkey")
  }

  sealed trait Definedness
  case object AnyDefined extends Definedness
  case object AllDefined extends Definedness
}

trait Scanner {
  type A
  def init: A
  def scan(a: A, cols: ColumnMap, range: Range): A -> ColumnMap
}

final class GroupId(val id: Int) extends AnyVal {
  override def toString = s"$id"
}
object GroupId {
  implicit def apply(id: Int): GroupId = new GroupId(id)
}

trait TransSpecModule {
  import trans._

  type TableTransSpec[+A <: SourceType] = Map[CPathField, TransSpec[A]]
  type TableTransSpec1                  = TableTransSpec[Source1]
  type TableTransSpec2                  = TableTransSpec[Source2]

  type F1 = CF1
  type F2 = CF2

  implicit def liftF1(f1: F1): F1Like
  implicit def liftF2(f2: F2): F2Like

  def lookupF1(namespace: List[String], name: String): F1
  def lookupF2(namespace: List[String], name: String): F2

  trait F1Like {
    def compose(f1: F1): F1
    def andThen(f1: F1): F1
  }
  trait F2Like {
    def applyl(cv: CValue): F1
    def applyr(cv: CValue): F1
    def andThen(f1: F1): F2
  }

  class TransSpecBuilder[A <: SourceType](val spec: TransSpec[A]) extends Dynamic {
    protected def next[A <: SourceType](x: TransSpec[A]): TransSpecBuilder[A] = new TransSpecBuilder(x)

    def isType(tp: JType): TransSpecBuilder[A]           = next(IsType(spec, tp))
    def apply(index: Int): TransSpecBuilder[A]           = next(DerefArrayStatic(spec, CPathIndex(index)))
    def delete(fields: CPathField*): TransSpecBuilder[A] = next(ObjectDelete(spec, fields.toSet))
    def select(name: String): TransSpecBuilder[A]        = next(DerefObjectStatic(spec, CPathField(name)))
    def selectDynamic(name: String): TransSpecBuilder[A] = select(name)
  }
  implicit class TransSpecOps[A <: SourceType](val spec: TransSpec[A]) {
    def inner_++(x: TransSpec[A], xs: TransSpec[A]*): InnerObjectConcat[A] = InnerObjectConcat(spec +: x +: xs: _*)
    def outer_++(x: TransSpec[A], xs: TransSpec[A]*): OuterObjectConcat[A] = OuterObjectConcat(spec +: x +: xs: _*)
  }

  implicit def transSpecBuilder[A <: SourceType](x: TransSpec[A]): TransSpecBuilder[A]       = new TransSpecBuilder(x)
  implicit def transSpecBuilderResult[A <: SourceType](x: TransSpecBuilder[A]): TransSpec[A] = x.spec

  object root extends TransSpecBuilder(Leaf(Source)) {
    def value = selectDynamic("value")
    def key   = selectDynamic("key")
  }
  object F1Expr {
    def negate         = lookupF1(Nil, "negate")
    def coerceToDouble = lookupF1(Nil, "coerceToDouble")
    def moduloTwo      = lookupF2(Nil, "mod") applyr CLong(2)
    def equalsZero     = lookupF2(Nil, "eq") applyr CLong(0)
    def isEven         = moduloTwo andThen equalsZero
  }
  object Fn {
    def source                    = Leaf(Source)
    def valueIsEven(name: String) = Map1(root select name, F1Expr.isEven)
    def constantTrue              = Filter(source, Equal(source, source))
  }

  object trans {
    sealed trait TransSpec[+A <: SourceType]  extends AnyRef
    sealed trait ObjectSpec[+A <: SourceType] extends TransSpec[A]
    sealed trait ArraySpec[+A <: SourceType]  extends TransSpec[A]

    sealed trait SourceType                   extends AnyRef
    sealed trait Source1                      extends SourceType
    sealed trait Source2                      extends SourceType
    case object Source                        extends Source1
    case object SourceLeft                    extends Source2
    case object SourceRight                   extends Source2

    type TransSpec1 = TransSpec[Source1]

    case class Leaf[+A <: SourceType](source: A) extends TransSpec[A] //done

    case class Filter[+A <: SourceType](source: TransSpec[A], predicate: TransSpec[A]) extends TransSpec[A] //done

    // Adds a column to the output in the manner of scanLeft
    case class Scan[+A <: SourceType](source: TransSpec[A], scanner: Scanner) extends TransSpec[A] //done

    case class Map1[+A <: SourceType](source: TransSpec[A], f: F1) extends TransSpec[A] //done

    case class DeepMap1[+A <: SourceType](source: TransSpec[A], f: F1) extends TransSpec[A] //done

    // apply a function to the cartesian product of the transformed left and right subsets of columns
    case class Map2[+A <: SourceType](left: TransSpec[A], right: TransSpec[A], f: F2) extends TransSpec[A] //done

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

      def mapSources[A <: SourceType, B <: SourceType](spec: TransSpec[A])(f: A => B): TransSpec[B] = {
        spec match {
          case Leaf(source)                      => Leaf(f(source))
          case trans.ConstLiteral(value, target) => trans.ConstLiteral(value, mapSources(target)(f))

          case trans.Filter(source, pred) => trans.Filter(mapSources(source)(f), mapSources(pred)(f))
          case trans.FilterDefined(source, definedFor, definedness) =>
            trans.FilterDefined(mapSources(source)(f), mapSources(definedFor)(f), definedness)

          case Scan(source, scanner) => Scan(mapSources(source)(f), scanner)
          // case MapWith(source, mapper) => MapWith(mapSources(source)(f), mapper)

          case trans.Map1(source, f1)      => trans.Map1(mapSources(source)(f), f1)
          case trans.DeepMap1(source, f1)  => trans.DeepMap1(mapSources(source)(f), f1)
          case trans.Map2(left, right, f2) => trans.Map2(mapSources(left)(f), mapSources(right)(f), f2)

          case trans.OuterObjectConcat(objects @ _ *) => trans.OuterObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.InnerObjectConcat(objects @ _ *) => trans.InnerObjectConcat(objects.map(mapSources(_)(f)): _*)
          case trans.ObjectDelete(source, fields)     => trans.ObjectDelete(mapSources(source)(f), fields)
          case trans.InnerArrayConcat(arrays @ _ *)   => trans.InnerArrayConcat(arrays.map(mapSources(_)(f)): _*)
          case trans.OuterArrayConcat(arrays @ _ *)   => trans.OuterArrayConcat(arrays.map(mapSources(_)(f)): _*)

          case trans.WrapObject(source, field)      => trans.WrapObject(mapSources(source)(f), field)
          case trans.WrapObjectDynamic(left, right) => trans.WrapObjectDynamic(mapSources(left)(f), mapSources(right)(f))
          case trans.WrapArray(source)              => trans.WrapArray(mapSources(source)(f))

          case DerefMetadataStatic(source, field) => DerefMetadataStatic(mapSources(source)(f), field)

          case DerefObjectStatic(source, field)  => DerefObjectStatic(mapSources(source)(f), field)
          case DerefObjectDynamic(left, right)   => DerefObjectDynamic(mapSources(left)(f), mapSources(right)(f))
          case DerefArrayStatic(source, element) => DerefArrayStatic(mapSources(source)(f), element)
          case DerefArrayDynamic(left, right)    => DerefArrayDynamic(mapSources(left)(f), mapSources(right)(f))

          case trans.ArraySwap(source, index) => trans.ArraySwap(mapSources(source)(f), index)

          case Typed(source, tpe)         => Typed(mapSources(source)(f), tpe)
          case TypedSubsumes(source, tpe) => TypedSubsumes(mapSources(source)(f), tpe)
          case IsType(source, tpe)        => IsType(mapSources(source)(f), tpe)

          case trans.Equal(left, right)                  => trans.Equal(mapSources(left)(f), mapSources(right)(f))
          case trans.EqualLiteral(source, value, invert) => trans.EqualLiteral(mapSources(source)(f), value, invert)

          case trans.Cond(pred, left, right) => trans.Cond(mapSources(pred)(f), mapSources(left)(f), mapSources(right)(f))
        }
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

        case DerefObjectStatic(source, field)  => DerefObjectStatic(deepMap(source)(f), field)
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

      val Id              = Leaf(Source)
      val DerefArray0     = Id(0)
      val DerefArray1     = Id(1)
      val DerefArray2     = Id(2)
      val PruneToKeyValue = WrapObject(SourceKey.Single, paths.Key.name) inner_++ WrapObject(SourceValue.Single, paths.Value.name)
      val DeleteKeyValue  = Id.delete(paths.Key, paths.Value)
    }

    type TransSpec2 = TransSpec[Source2]

    object TransSpec2 {
      val LeftId = Leaf(SourceLeft)

      /** Flips all `SourceLeft`s to `SourceRight`s and vice versa. */
      def flip(spec: TransSpec2): TransSpec2 = TransSpec.mapSources(spec) {
        case SourceLeft  => SourceRight
        case SourceRight => SourceLeft
      }

      def DerefArray0(source: Source2) = Leaf(source)(0)
      def DerefArray1(source: Source2) = Leaf(source)(1)
      def DerefArray2(source: Source2) = Leaf(source)(2)

      val DeleteKeyValueLeft  = Leaf(SourceLeft).delete(paths.Key, paths.Value)
      val DeleteKeyValueRight = Leaf(SourceRight).delete(paths.Key, paths.Value)
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
      import paths._

      object SourceKey {
        val Single = DerefObjectStatic(Leaf(Source), Key)
        val Left   = DerefObjectStatic(Leaf(SourceLeft), Key)
        val Right  = DerefObjectStatic(Leaf(SourceRight), Key)
      }

      object SourceValue {
        val Single = DerefObjectStatic(Leaf(Source), Value)
        val Left   = DerefObjectStatic(Leaf(SourceLeft), Value)
        val Right  = DerefObjectStatic(Leaf(SourceRight), Value)
      }
    }
  }
}
