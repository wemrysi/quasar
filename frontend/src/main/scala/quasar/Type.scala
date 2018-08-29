/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar

import slamdata.Predef._
import quasar.common.PrimaryType
import quasar.common.data.Data
import quasar.frontend.data.DataCodec
import quasar.fp._
import quasar.fp.ski._

import scala.Any

import argonaut._, Argonaut._, ArgonautScalaz._
import scalaz._, Scalaz._

sealed abstract class Type extends Product with Serializable { self =>
  import Type._

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  final def ⨯ (that: Type): Type =
    (this, that) match {
      case (t1, t2) if t1.contains(t2) => t2
      case (t1, t2) if t2.contains(t1) => t1
      case (Obj(m1, u1), Obj(m2, u2)) =>
        Obj(
          m1.unionWith(m2)(_ ⨯ _),
          Apply[Option].lift2((t1: Type, t2: Type) => t1 ⨯ t2)(u1, u2))
      case (FlexArr(min1, max1, t1), FlexArr(min2, max2, t2)) =>
        FlexArr(min1 + min2, (max1 |@| max2)(_ + _), t1 ⨿ t2)
      case (_, _)                     => Bottom
    }

  // FIXME: Using `≟` here causes runtime errors.
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  final def ⨿ (that: Type): Type =
    if (this == that) this else Coproduct(this, that)

  final def contains(that: Type): Boolean =
    typecheck(self, that).fold(κ(false), κ(true))

  def widenConst: Type = this match {
    case Type.Const(d) => dataType(d)
    case t => t
  }
}

trait TypeInstances {
  import Type._

  val TypeOrMonoid = new Monoid[Type] {
    def zero = Type.Bottom

    def append(v1: Type, v2: => Type) = (v1, v2) match {
      case (Type.Const(Data.NA) | Type.Bottom, that) => that
      case (this0, Type.Const(Data.NA) | Type.Bottom) => this0
      case (this0, that0) if this0.contains(Type.Top) || that0.contains(Type.Top) => Type.Top
      case (_, _) => v1 ⨿ v2
    }
  }

  val TypeGlbMonoid = new Monoid[Type] {
    def zero = Type.Top
    def append(f1: Type, f2: => Type) = Type.glb(f1, f2)
  }

  val TypeLubMonoid = new Monoid[Type] {
    def zero = Type.Bottom
    def append(f1: Type, f2: => Type) = Type.lub(f1, f2)
  }

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  implicit val show: Show[Type] = Show.show {
    case Const(d) => s"constant value ${d.shows}"
    case Arr(types) => "Arr(" + types.shows + ")"
    case FlexArr(min, max, mbrs) =>
      "FlexArr(" + min.shows + ", " + max.shows + ", "  + mbrs.shows + ")"
    case Obj(assocs, unkns) =>
      "Obj(" + assocs.shows + ", " + unkns.shows + ")"
    case cp @ Coproduct(_, _) =>
      val cos = cp.flatten.map(_.shows)
      cos.init.toList.mkString(", ") + ", or " + cos.last
    case x => x.toString
  }

  implicit val TypeRenderTree: RenderTree[Type] =
    RenderTree.fromShow[Type]("Type")

  implicit val typeEncodeJson: EncodeJson[Type] =
    EncodeJson {
      case Top =>
        jString("Top")
      case Bottom =>
        jString("Bottom")
      case Const(d) =>
        Json("Const" -> DataCodec.Precise.encode(d).getOrElse(jString("Undefined")))
      case Null =>
        jString("Null")
      case Str =>
        jString("Str")
      case Int =>
        jString("Int")
      case Dec =>
        jString("Dec")
      case Bool =>
        jString("Bool")
      case OffsetDateTime =>
        jString("OffsetDateTime")
      case OffsetTime =>
        jString("OffsetTime")
      case Type.OffsetDate =>
        jString("OffsetDate")
      case LocalDateTime =>
        jString("LocalDateTime")
      case LocalTime =>
        jString("LocalTime")
      case LocalDate =>
        jString("LocalDate")
      case Interval =>
        jString("Interval")
      case Arr(types) =>
        Json("Array" := types)
      case FlexArr(min, max, mbrs) =>
        val flexarr =
          ("minSize" :=  min) ->:
          ("maxSize" :?= max) ->?:
          ("members" :=  mbrs) ->:
          jEmptyObject
        Json("FlexArr" -> flexarr)
      case Obj(assocs, unkns) =>
        val obj =
          ("associations" :=  assocs) ->:
          ("unknownKeys"  :?= unkns)  ->?:
          jEmptyObject
        Json("Obj" -> obj)
      case cp @ Coproduct(l, r) =>
        Json("Coproduct" := cp.flatten)
    }
}

object Type extends TypeInstances {
  type UnificationResult[A] = ValidationNel[UnificationError, A]

  val fromPrimaryType: PrimaryType => Type = {
    case common.Null => Null
    case common.Bool => Bool
    case common.Char => Top
    case common.Int  => Int
    case common.Dec  => Dec
    case common.Arr  => Top
    case common.Map  => AnyObject
  }

  val dataType: Data => Type = {
    case Data.Null => Null
    case Data.Str(_) => Str
    case Data.Bool(_) => Bool
    case Data.Dec(_) => Dec
    case Data.Int(_) => Int
    case Data.Obj(v) => Obj(v.mapValues(Const(_)), None)
    case Data.Arr(v) => Arr(v.map(Const(_)))
    case Data.OffsetDateTime(_) => OffsetDateTime
    case Data.OffsetTime(_) => OffsetTime
    case Data.OffsetDate(_) => OffsetDate
    case Data.LocalDateTime(_) => LocalDateTime
    case Data.LocalTime(_) => LocalTime
    case Data.LocalDate(_) => LocalDate
    case Data.Interval(_) => Interval
    case Data.NA => Bottom
  }

  private def fail0[A](expected: Type, actual: Type, message: Option[String])
      : UnificationResult[A] =
    Validation.failure(NonEmptyList(UnificationError(expected, actual, message)))

  private def fail[A](expected: Type, actual: Type): UnificationResult[A] =
    fail0(expected, actual, None)

  private def failMsg[A](expected: Type, actual: Type, msg: String)
      : UnificationResult[A] =
    fail0(expected, actual, Some(msg))

  private def succeed[A](v: A): UnificationResult[A] = Validation.success(v)

  def simplify(tpe: Type): Type = mapUp(tpe) {
    case x @ Coproduct(_, _) => {
      val xs = x.flatten.toList
      val ts = xs.filter(t => t != Bottom && t != Type.Const(Data.NA))
      val hasNA = xs.contains(Const(Data.NA))
      if (ts.contains(Top)) Top
      else if (hasNA && ts == Nil) Const(Data.NA)
      else Coproduct.fromSeq(ts.distinct)
    }
    case x => x
  }

  def glb(left: Type, right: Type): Type = left ⨯ right

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def lub(left: Type, right: Type): Type = (left, right) match {
    case _ if left contains right   => left
    case _ if right contains left   => right
    case (Const(l), Const(r))       => lub(dataType(l), dataType(r))
    case (Obj(v1, u1), Obj(v2, u2)) =>
      Obj(
        v1.unionWith(v2)(lub),
        u1.fold(u2)(unk => u2.fold(u1)(lub(unk, _).some)))
    case _                          => Top
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def constructiveLub(left: Type, right: Type): Type = (left, right) match {
    case _ if left contains right   => left
    case _ if right contains left   => right
    case (Const(l), Const(r))       => lub(dataType(l), dataType(r))
    case _                          => left ⨿ right
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def typecheck(superType: Type, subType: Type):
      UnificationResult[Unit] =
    (superType, subType) match {
      case (superType, subType) if (superType ≟ subType) => succeed(())

      case (Top, _)    => succeed(())
      case (_, Bottom) => succeed(())
      case (_, Top)    => failMsg(superType, subType, "Top is not a subtype of anything")
      case (Bottom, _) => failMsg(superType, subType, "Bottom is not a supertype of anything")

      case (superType @ Coproduct(_, _), subType @ Coproduct(_, _)) =>
        typecheckCC(superType.flatten.toVector, subType.flatten.toVector)
      case (Arr(elem1), Arr(elem2)) =>
        if (elem1.length <= elem2.length)
          Zip[List].zipWith(elem1, elem2)(typecheck).concatenate
        else failMsg(superType, subType, "subtype must be at least as long")
      case (FlexArr(supMin, supMax, superType), Arr(elem2))
          if supMin <= elem2.length =>
        typecheck(superType, elem2.concatenate(TypeOrMonoid))
      case (FlexArr(supMin, supMax, superType), FlexArr(subMin, subMax, subType)) =>
        lazy val tc = typecheck(superType, subType)
        def checkOpt[A](sup: Option[A], comp: (A, A) => Boolean, sub: Option[A], next: => UnificationResult[Unit]) =
          sup.fold(
            next)(
            p => sub.fold[UnificationResult[Unit]](
              fail(superType, subType))(
              b => if (comp(p, b)) next else fail(superType, subType)))
        lazy val max = checkOpt(supMax, Order[Int].greaterThanOrEqual, subMax, tc)
        checkOpt(Some(supMin), Order[Int].lessThanOrEqual, Some(subMin), max)
      case (Obj(supMap, supUk), Obj(subMap, subUk)) =>
        supMap.toList.foldMap { case (k, v) =>
          subMap.get(k).fold[UnificationResult[Unit]](
            fail(superType, subType))(
            typecheck(v, _))
        } +++
          supUk.fold(
            subUk.fold[UnificationResult[Unit]](
              if ((subMap -- supMap.keySet).isEmpty) succeed(()) else fail(superType, subType))(
              κ(fail(superType, subType))))(
            p => subUk.fold[UnificationResult[Unit]](
              // if (subMap -- supMap.keySet) is empty, fail(superType, subType)
              (subMap -- supMap.keySet).foldMap(typecheck(p, _)))(
              typecheck(p, _)))

      case (superType, subType @ Coproduct(_, _)) =>
        typecheckPC(superType, subType.flatten.toVector)

      case (superType @ Coproduct(_, _), subType) =>
        typecheckCP(superType.flatten.toVector, subType)

      case (superType, Const(subType)) => typecheck(superType, dataType(subType))

      case _ => fail(superType, subType)
    }

  def children(v: Type): List[Type] = v match {
    case Top => Nil
    case Bottom => Nil
    case Const(value) => dataType(value) :: Nil
    case Null => Nil
    case Str => Nil
    case Int => Nil
    case Dec => Nil
    case Bool => Nil
    case OffsetDateTime => Nil
    case OffsetTime => Nil
    case OffsetDate => Nil
    case LocalDateTime => Nil
    case LocalTime => Nil
    case LocalDate => Nil
    case Interval => Nil
    case Arr(value) => value
    case FlexArr(_, _, value) => value :: Nil
    case Obj(map, uk) => uk.toList ++ map.values.toList
    case x @ Coproduct(_, _) => x.flatten.toList
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def foldMap[Z: Monoid](f: Type => Z)(v: Type): Z =
    Monoid[Z].append(f(v), children(v).foldMap(foldMap(f)))

  def mapUp(v: Type)(f: PartialFunction[Type, Type]): Type = {
    val f0 = f.orElse[Type, Type] {
      case x => x
    }

    mapUpM[scalaz.Id.Id](v)(f0)
  }

  def mapUpM[F[_]: Monad](v: Type)(f: Type => F[Type]): F[Type] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(v: Type): F[Type] = v match {
      case Const(value) =>
         for {
          newType  <- f(dataType(value))
          newType2 <- if (newType ≠ dataType(value)) Monad[F].point(newType)
                      else f(v)
        } yield newType2

      case FlexArr(min, max, value) => wrap(value, FlexArr(min, max, _))
      case Arr(value)               => value.traverse(f).map(Arr)
      case Obj(map, uk)             =>
        (map.traverse(f) |@| uk.traverse(f))(Obj)

      case x @ Coproduct(_, _) =>
        for {
          xs <- x.flatten.toList.traverse(loop)
          v2 <- f(Coproduct.fromSeq(xs))
        } yield v2

      case _ => f(v)
    }

    def wrap(v0: Type, constr: Type => Type) =
      for {
        v1 <- loop(v0)
        v2 <- f(constr(v1))
      } yield v2

    loop(v)
  }

  final case object Top               extends Type
  final case object Bottom            extends Type

  final case class Const(value: Data) extends Type

  final case object Null              extends Type
  final case object Str               extends Type
  final case object Int               extends Type
  final case object Dec               extends Type
  final case object Bool              extends Type
  final case object OffsetDateTime    extends Type
  final case object OffsetTime        extends Type
  final case object OffsetDate        extends Type
  final case object LocalDateTime     extends Type
  final case object LocalTime         extends Type
  final case object LocalDate         extends Type
  final case object Interval          extends Type

  final case class Arr(value: List[Type]) extends Type
  final case class FlexArr(minSize: Int, maxSize: Option[Int], value: Type)
      extends Type

  // NB: `unknowns` represents the type of any values where we don’t know the
  //      keys. None means the Obj is fully known.
  final case class Obj(value: Map[String, Type], unknowns: Option[Type])
      extends Type

  final case class Coproduct(left: Type, right: Type) extends Type {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def flatten: NonEmptyList[Type] = {
      def flatten0(v: Type): NonEmptyList[Type] = v match {
        case left ⨿ right => flatten0(left) append flatten0(right)
        case x            => NonEmptyList(x)
      }

      flatten0(this)
    }

    override def hashCode = flatten.toSet.hashCode()

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    override def equals(that: Any) = that match {
      case that @ Coproduct(_, _) =>
        this.flatten.toSet.equals(that.flatten.toSet)
      case _ => false
    }
  }
  object Coproduct {
    def fromSeq(values: Seq[Type]): Type =
      values.reduceLeftOption(_ ⨿ _).getOrElse(Bottom)
  }

  object ⨿ {
    def unapply(obj: Type): Option[(Type, Type)] = obj match {
      case Coproduct(a, b) => (a, b).some
      case _               => None
    }
  }

  private def typecheckPC(expected: Type, actuals: Vector[Type]) =
    actuals.foldMap(typecheck(expected, _))

  private def typecheckCP(expecteds: Vector[Type], actual: Type) =
    expecteds.foldLeft[UnificationResult[Unit]](
      fail(Bottom, actual))(
      (acc, expected) => acc ||| typecheck(expected, actual))

  private def typecheckCC(expecteds: Vector[Type], actuals: Vector[Type]) =
    actuals.foldMap(typecheckCP(expecteds, _))

  val AnyArray = FlexArr(0, None, Top)
  val AnyObject = Obj(Map(), Some(Top))
  val Numeric = Int ⨿ Dec
  val Temporal = OffsetDateTime ⨿ OffsetDate ⨿ OffsetTime ⨿ LocalDateTime ⨿ LocalDate ⨿ LocalTime
  val Comparable = Numeric ⨿ Str ⨿ Temporal ⨿ Bool
  val Syntaxed = Type.Null ⨿ Type.Comparable ⨿ Type.Interval
  val HasDate = OffsetDateTime ⨿ OffsetDate ⨿ LocalDateTime ⨿ LocalDate
  val HasTime = OffsetDateTime ⨿ OffsetTime ⨿ LocalDateTime ⨿ LocalTime
  val HasOffset = OffsetDateTime ⨿ OffsetDate ⨿ OffsetTime

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Recursion"))
  implicit val equal: Equal[Type] = Equal.equal((a, b) => (a, b) match {
    case (Top, Top)
       | (Bottom, Bottom)
       | (Null, Null)
       | (Str, Str)
       | (Int, Int)
       | (Dec, Dec)
       | (Bool, Bool)
       | (OffsetDateTime, OffsetDateTime)
       | (OffsetTime, OffsetTime)
       | (OffsetDate, OffsetDate)
       | (LocalDateTime, LocalDateTime)
       | (LocalTime, LocalTime)
       | (LocalDate, LocalDate)
       | (Interval, Interval) =>
      true
    case (Const(a), Const(b)) => a ≟ b
    case (Arr(as), Arr(bs)) => as ≟ bs
    case (FlexArr(min1, max1, t1), FlexArr(min2, max2, t2)) =>
      min1 ≟ min2 && max1 ≟ max2 && t1 ≟ t2
    case (Obj(v1, u1), Obj(v2, u2)) => v1 ≟ v2 && u1 ≟ u2
    case (a @ Coproduct(_, _), b @ Coproduct(_, _)) => a.equals(b)
    case (_, _) => false
  })
}
