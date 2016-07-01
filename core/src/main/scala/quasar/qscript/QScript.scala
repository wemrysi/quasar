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

package quasar.qscript

import quasar._
import quasar.ejson
import quasar.ejson.{Int => _, _}
import quasar.fp._
import quasar.qscript.MapFuncs._
import quasar.Predef._
import quasar.std.StdLib._

import scala.Predef.implicitly

import matryoshka._, FunctorT.ops._, Recursive.ops._
import matryoshka.patterns._
import pathy.Path._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._
import shapeless.{:: => _, Data => _, Coproduct => _, Const => _, _}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - each `Free` structure in a *Join or Union will have exactly one `point`
// - the common source in a Join or Union will be the longest common branch
// - all Reads have a Root (or another Read?) as their source
// - in `Pathable`, the only `MapFunc` node allowed is a `ProjectField`

sealed trait SortDir
final case object Ascending  extends SortDir
final case object Descending extends SortDir

// TODO: Just reuse the version of this from LP?
object SortDir {
  implicit val equal: Equal[SortDir] = Equal.equalRef
  implicit val show: Show[SortDir] = Show.showFromToString
}

sealed trait JoinType
final case object Inner extends JoinType
final case object FullOuter extends JoinType
final case object LeftOuter extends JoinType
final case object RightOuter extends JoinType

object JoinType {
  implicit val equal: Equal[JoinType] = Equal.equalRef
  implicit val show: Show[JoinType] = Show.showFromToString
}

sealed trait DeadEnd

object DeadEnd {
  implicit def equal: Equal[DeadEnd] = Equal.equalRef
  implicit def show: Show[DeadEnd] = Show.showFromToString

  implicit def mergeable[T[_[_]]]: Mergeable.Aux[T, DeadEnd] =
    new Mergeable[DeadEnd] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: DeadEnd,
        p2: DeadEnd):
          Option[Merge[IT, DeadEnd]] =
        if (p1 === p2)
          Some(AbsMerge[IT, DeadEnd, FreeMap](p1, UnitF, UnitF))
        else
          None
    }
}

/** The top level of a filesystem. During compilation this represents `/`, but
  * in the structure a backend sees, it represents the mount point.
  */
final case object Root extends DeadEnd
final case object Empty extends DeadEnd

/** A backend-resolved `Root`, which is now a path. */
final case class Read[A](src: A, path: AbsFile[Sandboxed])

object Read {
  implicit def equal[T[_[_]]]: Delay[Equal, Read] =
    new Delay[Equal, Read] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (Read(a1, p1), Read(a2, p2)) => eq.equal(a1, a2) && p1 ≟ p2
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[Read] =
    new Traverse[Read] {
      def traverseImpl[G[_]: Applicative, A, B](fa: Read[A])(f: A => G[B]) =
        f(fa.src) ∘ (Read(_, fa.path))
    }
}

// backends can choose to rewrite joins using EquiJoin
// can rewrite a ThetaJoin as EquiJoin + Filter
final case class EquiJoin[T[_[_]], A](
  lKey: FreeMap[T],
  rKey: FreeMap[T],
  f: JoinType,
  src: A,
  lBranch: FreeQS[T],
  rBranch: FreeQS[T])

object EquiJoin {
  implicit def equal[T[_[_]]](implicit eqTEj: Equal[T[EJson]]):
      Delay[Equal, EquiJoin[T, ?]] =
    new Delay[Equal, EquiJoin[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (EquiJoin(lk1, rk1, f1, a1, l1, r1),
                EquiJoin(lk2, rk2, f2, a2, l2, r2)) =>
            lk1 ≟ lk2 && rk1 ≟ rk2 && f1 ≟ f2 && eq.equal(a1, a2) && l1 ≟ l2 && r1 ≟ r2
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[EquiJoin[T, ?]] =
    new Traverse[EquiJoin[T, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](
        fa: EquiJoin[T, A])(
        f: A => G[B]) =
        f(fa.src) ∘
          (EquiJoin(fa.lKey, fa.rKey, fa.f, _, fa.lBranch, fa.rBranch))
    }
}

trait Helpers[T[_[_]]] {
  def basicJF: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))
}

class Transform[T[_[_]]: Recursive: Corecursive](
    implicit showInner: Show[T[QScriptInternal[T, ?]]],
             eqTEj: Equal[T[EJson]]) extends Helpers[T] {

  type Inner = T[QScriptInternal[T, ?]]
  type InnerPure = T[QScriptPure[T, ?]]

  type Pures[A] = List[QScriptInternal[T, A]]
  type DoublePures = (Pures[Unit], Pures[Unit])
  type TriplePures = (Pures[Unit], Pures[Unit], Pures[Unit])

  type DoubleFreeMap = (FreeMap[T], FreeMap[T])

  val DeadEndInternal = implicitly[Const[DeadEnd, ?] :<: QScriptInternal[T, ?]]
  val SourcedPathableInternal = implicitly[SourcedPathable[T, ?] :<: QScriptInternal[T, ?]]
  val QScriptCoreInternal = implicitly[QScriptCore[T, ?] :<: QScriptInternal[T, ?]]
  val ThetaJoinInternal = implicitly[ThetaJoin[T, ?] :<: QScriptInternal[T, ?]]
  val QScriptBucketInternal = implicitly[QScriptBucket[T, ?] :<: QScriptInternal[T, ?]]

  def linearize[F[_]: Functor: Foldable]: Algebra[F, List[F[Unit]]] =
    fl => fl.void :: fl.fold

  def delinearizeInner[F[_]: Functor, A](implicit DE: Const[DeadEnd, ?] :<: F):
      Coalgebra[F, List[F[A]]] = {
    case Nil    => DE.inj(Const(Root))
    case h :: t => h.map(_ => t)
  }

  def delinearizeFreeQS[F[_]: Functor, A]:
      ElgotCoalgebra[Unit \/ ?, F, List[F[A]]] = {
    case Nil    => ().left
    case h :: t => h.map(_ => t).right
  }

  val consZipped: Algebra[ListF[QScriptInternal[T, Unit], ?], TriplePures] = {
    case NilF() => (Nil, Nil, Nil)
    case ConsF(head, (acc, l, r)) => (head :: acc, l, r)
  }

  val zipper: ElgotCoalgebra[TriplePures \/ ?, ListF[QScriptInternal[T, Unit], ?], (DoubleFreeMap, DoublePures)] = {
    case ((_, _), (Nil, Nil)) => (Nil, Nil, Nil).left
    case ((_, _), (Nil, r))   => (Nil, Nil, r).left
    case ((_, _), (l,   Nil)) => (Nil, l,   Nil).left
    case ((lm, rm), (l :: ls, r :: rs)) => {
      val ma = implicitly[Mergeable.Aux[T, QScriptInternal[T, Unit]]]

      ma.mergeSrcs(lm, rm, l, r).fold[TriplePures \/ ListF[QScriptInternal[T, Unit], (DoubleFreeMap, DoublePures)]](
        (Nil, l :: ls, r :: rs).left) {
        case AbsMerge(inn, lmf, rmf) => ConsF(inn, ((lmf, rmf), (ls, rs))).right[TriplePures]
      }
    }
  }

  def merge(left: Inner, right: Inner): MergeJoin[T, Inner] = {
    val lLin: Pures[Unit] = left.cata(linearize).reverse
    val rLin: Pures[Unit] = right.cata(linearize).reverse

    val (common, lTail, rTail) =
      ((UnitF[T], UnitF[T]), (lLin, rLin)).elgot(consZipped, zipper)

    AbsMerge[T, Inner, FreeQS](
      common.reverse.ana[T, QScriptInternal[T, ?]](delinearizeInner),
      foldIso(CoEnv.freeIso[Unit, QScriptInternal[T, ?]])
        .get(lTail.reverse.ana[T, CoEnv[Unit, QScriptInternal[T, ?], ?]](delinearizeFreeQS[QScriptInternal[T, ?], Unit] >>> (CoEnv(_)))),
      foldIso(CoEnv.freeIso[Unit, QScriptInternal[T, ?]])
        .get(rTail.reverse.ana[T, CoEnv[Unit, QScriptInternal[T, ?], ?]](delinearizeFreeQS[QScriptInternal[T, ?], Unit] >>> (CoEnv(_)))))
  }

  def merge2Map(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      SourcedPathable[T, Inner] = {
    val AbsMerge(merged, left, right) = merge(values(0), values(1))
    val res = makeBasicTheta(merged, left, right)

    Map(ThetaJoinInternal.inj(res.src).embed, Free.roll(func(res.left, res.right)))
  }

  def merge2Expansion(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      SourcedPathable[T, Inner] = {
    val AbsMerge(merged, left, right) = merge(values(0), values(1))
    val res = makeBasicTheta(merged, left, right)

    LeftShift(
      ThetaJoinInternal.inj(res.src).embed,
      Free.roll(func(res.left, res.right)),
      Free.point(RightSide)) // TODO Does this add a bucket for the range values? (values mirrored in buckets)
  }

  def merge3Map(
    values: Func.Input[Inner, nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]])(
    implicit ma: Mergeable.Aux[T, QScriptInternal[T, Unit]]):
      SourcedPathable[T, Inner] = {

    val AbsMerge(merged, first, second) = merge(values(0), values(1))
    val AbsMerge(merged2, fands, third) = merge(merged, values(2))

    val res = makeBasicTheta(merged2, first, second)
    val res2 = makeBasicTheta(ThetaJoinInternal.inj(res.src).embed, fands, third)

    Map(ThetaJoinInternal.inj(res2.src).embed, Free.roll(func(
      rebase(res2.left, res.left),
      rebase(res2.left, res.right),
      res2.right)))
  }

  // TODO namegen
  def makeBasicTheta[A](src: A, left: FreeQS[T], right: FreeQS[T]):
      Merge[T, ThetaJoin[T, A]] =
    AbsMerge[T, ThetaJoin[T, A], FreeMap](
      ThetaJoin(src, left, right, basicJF, Inner,
        Free.roll(ConcatObjects(
          Free.roll(MakeObject(
            Free.roll(StrLit("tmp1")),
            Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
          Free.roll(MakeObject(
            Free.roll(StrLit("tmp2")),
            Free.point[MapFunc[T, ?], JoinSide](RightSide)))))),
      Free.roll(ProjectField(UnitF[T], Free.roll(StrLit("tmp1")))),
      Free.roll(ProjectField(UnitF[T], Free.roll(StrLit("tmp2")))))

  // NB: More compilicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map((), mf), LeftShift((), struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(
      func: UnaryFunc,
      values: Func.Input[Inner, nat._1]):
    QScriptInternal[T, Inner] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap =>
        SourcedPathableInternal.inj(LeftShift(
          values(0),
          UnitF,
          Free.point(RightSide)))

      case structural.FlattenArray =>
        SourcedPathableInternal.inj(LeftShift(
          values(0),
          UnitF,
          Free.point(RightSide)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenMapKeys =>
        SourcedPathableInternal.inj(LeftShift(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide)))
      case structural.FlattenArrayIndices =>
        SourcedPathableInternal.inj(LeftShift(
          values(0),
          Free.roll(DupArrayIndices(UnitF)),
          Free.point(RightSide)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftMap =>
        QScriptBucketInternal.inj(LeftShiftBucket(
          values(0),
          UnitF,
          Free.point(RightSide),
          Free.roll(DupMapKeys(UnitF)))) // affects bucketing metadata
      case structural.ShiftArray =>
        QScriptBucketInternal.inj(LeftShiftBucket(
          values(0),
          UnitF,
          Free.point(RightSide),
          Free.roll(DupArrayIndices(UnitF)))) // affects bucketing metadata

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftMapKeys =>
        QScriptBucketInternal.inj(LeftShiftBucket(
          values(0),
          Free.roll(DupMapKeys(UnitF)),
          Free.point(RightSide),
          UnitF)) // affects bucketing metadata
      case structural.ShiftArrayIndices =>
        QScriptBucketInternal.inj(LeftShiftBucket(
          values(0),
          Free.roll(DupArrayIndices(UnitF)),
          Free.point(RightSide),
          UnitF)) // affects bucketing metadata
    }

  def invokeExpansion2(
      func: BinaryFunc,
      values: Func.Input[Inner, nat._2]): SourcedPathable[T, Inner] =
    func match {
      case set.Range => merge2Expansion(values)(Range(_, _))
    }

  def invokeMapping1(func: UnaryFunc, values: Func.Input[Inner, nat._1]):
      SourcedPathable[T, Inner] =
    Map(values(0), Free.roll(MapFunc.translateUnaryMapping(func)(UnitF)))

  def invokeMapping2(
    func: BinaryFunc,
    values: Func.Input[Inner, nat._2]): SourcedPathable[T, Inner] =
    merge2Map(values)(MapFunc.translateBinaryMapping(func))

  def invokeMapping3(
    func: TernaryFunc,
    values: Func.Input[Inner, nat._3]): SourcedPathable[T, Inner] =
    merge3Map(values)(MapFunc.translateTernaryMapping(func))

  def invokeReduction1(
      func: UnaryFunc,
      values: Func.Input[Inner, nat._1]):
    QScriptCore[T, Inner] = {

    val bucketable = implicitly[Bucketable.Aux[T, QScriptInternal[T, ?]]]

    val bucketDisj: QScriptBucket[T, Inner] \/ (Int, Inner) =
      values(0).cataM(bucketable.digForBucket).run(1)

    val (src, bucket, reduce): (Inner, FreeMap[T], FreeMap[T]) = bucketDisj match {
      case -\/(GroupBy(src, _, bucket)) =>
        val AbsMerge(merged, left, right) = merge(src, values(0))
        val res = makeBasicTheta(merged, left, right)
        (ThetaJoinInternal.inj(res.src).embed, rebase(bucket, res.left), res.right)
      case -\/(BucketField(src, _, name)) =>
        val AbsMerge(merged, left, right) = merge(src, values(0))
        val res = makeBasicTheta(merged, left, right)
        (ThetaJoinInternal.inj(res.src).embed, rebase(name, res.left), res.right)
      case -\/(BucketIndex(src, _, index)) =>
        val AbsMerge(merged, left, right) = merge(src, values(0))
        val res = makeBasicTheta(merged, left, right)
        (ThetaJoinInternal.inj(res.src).embed, rebase(index, res.left), res.right)
      case -\/(LeftShiftBucket(src, struct, _, bucket)) =>
        val AbsMerge(merged, left, right) = merge(src, values(0))
        val res = makeBasicTheta(merged, left, right)
        (ThetaJoinInternal.inj(res.src).embed, rebase(rebase(struct, bucket), res.left), res.right)
      // TODO in the cases below do we need to merge src?
      case -\/(SquashBucket(src)) =>
        (values(0), Free.roll(Nullary(CommonEJson.inj(ejson.Null[T[EJson]]()).embed)), UnitF) // singleton provenance - one big bucket
      case \/-((qs, 0)) =>
        (values(0), Free.roll(Nullary(CommonEJson.inj(ejson.Null[T[EJson]]()).embed)), UnitF) // singleton provenance - one big bucket
      case \/-((qs, _)) =>
        (values(0), UnitF, UnitF) // everying in an individual bucket - throw away reduce
    }

    Reduce[T, Inner, nat._0](
      src,
      bucket,
      Sized[List](ReduceFunc.translateReduction[FreeMap[T]](func)(reduce)),
      Free.point(Fin[nat._0, nat._1]))
  }

  def invokeThetaJoin(input: Func.Input[Inner, nat._3], tpe: JoinType): ThetaJoin[T, Inner] = {
    val AbsMerge(src1, jb1l, jb1r) = merge(input(0), input(1))
    val AbsMerge(src2, jb2l, jb2r) = merge(src1, input(2))

    val leftBr = rebase(jb2l, jb1l)
    val rightBr = rebase(jb2l, jb1r)

    val on: JoinFunc[T] = basicJF // TODO use jb2r

    ThetaJoin(src2, leftBr, rightBr, on, Inner, Free.point(LeftSide))
  }

  def pathToProj(path: pathy.Path[_, _, _]): FreeMap[T] =
    pathy.Path.peel(path).fold[FreeMap[T]](
      Free.point(())) {
      case (p, n) =>
        Free.roll(ProjectField(pathToProj(p),
          Free.roll(StrLit(n.fold(_.value, _.value)))))
    }

  // TODO error handling
  def fromData[T[_[_]]: Corecursive](data: Data): String \/ T[EJson] = {
    data.hyloM[String \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[String \/ ?, EJson, Data, T[EJson]]({
        case data => data.toString.left[T[EJson]]
      },
        _.embed.right[String]),
      Data.toEJson[EJson].apply(_).right)
  }

  def lpToQScript: LogicalPlan[Inner] => QScriptInternal[T, Inner] = {
    case LogicalPlan.ReadF(path) =>
      SourcedPathableInternal.inj(Map(
        CorecursiveOps[T, QScriptInternal[T, ?]](DeadEndInternal.inj(Const[DeadEnd, Inner](Root))).embed,
        pathToProj(path)))

    case LogicalPlan.ConstantF(data) => SourcedPathableInternal.inj(Map(
      DeadEndInternal.inj(Const[DeadEnd, Inner](Root)).embed,
      Free.roll[MapFunc[T, ?], Unit](Nullary[T, FreeMap[T]](fromData(data).fold(
        error => CommonEJson.inj(ejson.Str[T[EJson]](error)).embed,
        x => x)))))

    case LogicalPlan.FreeF(name) => SourcedPathableInternal.inj(Map(
      DeadEndInternal.inj(Const[DeadEnd, Inner](Empty)).embed,
      Free.roll(ProjectField(Free.roll(StrLit(name.toString)), UnitF[T]))))

    // TODO namegen
    case LogicalPlan.LetF(name, form, body) =>
      val AbsMerge(src, jb1, jb2) = merge(form, body)
      makeBasicTheta(src, jb1, jb2) match {
        case AbsMerge(src, fm1, fm2) =>
          SourcedPathableInternal.inj(Map(
            SourcedPathableInternal.inj(Map(
              ThetaJoinInternal.inj(src).embed,
              Free.roll(ConcatObjects(
                Free.roll(MakeObject(Free.roll(StrLit("tmp1")), UnitF[T])),
                Free.roll(MakeObject(Free.roll(StrLit(name.toString)), fm1)))))).embed,
            rebase(fm2, Free.roll(ProjectField(UnitF[T], Free.roll(StrLit("tmp1")))))))
      }

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      SourcedPathableInternal.inj(merge3Map(
        Func.Input3(expr, cont, fallback))(
        Guard(_, typ, _, _)))

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Mapping =>
      SourcedPathableInternal.inj(invokeMapping1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val AbsMerge(merged, left, right) = merge(a1, a2)
      val res = makeBasicTheta(merged, left, right)
      QScriptBucketInternal.inj(BucketField(
        ThetaJoinInternal.inj(res.src).embed,
        res.left,
        res.right))

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val AbsMerge(merged, left, right) = merge(a1, a2)
      val res = makeBasicTheta(merged, left, right)
      QScriptBucketInternal.inj(BucketIndex(
        ThetaJoinInternal.inj(res.src).embed,
        res.left,
        res.right))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Mapping =>
      SourcedPathableInternal.inj(invokeMapping2(func, Func.Input2(a1, a2)))

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3)) if func.effect == Mapping =>
      SourcedPathableInternal.inj(invokeMapping3(func, Func.Input3(a1, a2, a3)))

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Reduction =>
      QScriptCoreInternal.inj(invokeReduction1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      QScriptCoreInternal.inj(Take(src, jb1, jb2))

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      QScriptCoreInternal.inj(Drop(src, jb1, jb2))

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) =>
      val AbsMerge(merged, first, second) = merge(a1, a2)
      val AbsMerge(merged2, fands, third) = merge(merged, a3)

      val res = makeBasicTheta(merged, first, second)
      val res2 = makeBasicTheta(ThetaJoinInternal.inj(res.src), fands, third)

      val src = res2.src
      val data = rebase(res2.left, res.left)
      val providedBucket = rebase(res2.left, res.right)
      val order = res2.right

      ??? // TODO

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)

      makeBasicTheta(src, jb1, jb2) match {
        case AbsMerge(src, fm1, fm2) =>
          SourcedPathableInternal.inj(Map(
            QScriptCoreInternal.inj(Filter(ThetaJoinInternal.inj(src).embed, fm2)).embed,
            fm1))
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Squashing =>
      func match {
        case identity.Squash => QScriptBucketInternal.inj(SquashBucket(a1))
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Expansion =>
      invokeExpansion1(func, Func.Input1(a1))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Expansion =>
      SourcedPathableInternal.inj(invokeExpansion2(func, Func.Input2(a1, a2)))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Transformation =>
      func match {
        case set.GroupBy =>
          val AbsMerge(merged0, source0, bucket0) = merge(a1, a2)
          makeBasicTheta(merged0, source0, bucket0) match {
            case AbsMerge(merged, source, bucket) =>
              QScriptBucketInternal.inj(GroupBy(ThetaJoinInternal.inj(merged).embed, source, bucket))
          }
        case set.Union =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          SourcedPathableInternal.inj(Union(src, jb1, jb2))
        case set.Intersect =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          ThetaJoinInternal.inj(ThetaJoin(src, jb1, jb2, basicJF, Inner, Free.point(LeftSide)))
        case set.Except =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          ThetaJoinInternal.inj(ThetaJoin(
            src,
            jb1,
            jb2,
            Free.roll(Nullary(CommonEJson.inj(ejson.Bool[T[EJson]](false)).embed)),
            LeftOuter,
            Free.point(LeftSide)))
      }

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))  if func.effect == Transformation=>
      def invoke(tpe: JoinType): QScriptInternal[T, Inner] =
        ThetaJoinInternal.inj(invokeThetaJoin(Func.Input3(a1, a2, a3), tpe))

      //scala.Predef.println(s"left:  ${a1.shows}")
      //scala.Predef.println(s"right: ${a2.shows}")
      //scala.Predef.println(s"cond:  ${a3.transCata(liftFG((new Optimize[T]).elideNopJoins[QScriptInternal[T, ?]])).shows}")

      func match {
        case set.InnerJoin => invoke(Inner)
        case set.LeftOuterJoin => invoke(LeftOuter)
        case set.RightOuterJoin => invoke(RightOuter)
        case set.FullOuterJoin => invoke(FullOuter)
      }
  }
}

class Optimize[T[_[_]]: Recursive](
    implicit eqTEj: Equal[T[EJson]]) extends Helpers[T] {

  def elideNopMaps[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ UnitF => src.project
    case x                          => SP.inj(x)
  }

  def elideNopJoins[F[_]](
    implicit Th: ThetaJoin[T, ?] :<: F,
    SP: SourcedPathable[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] = {
    case ThetaJoin(src, l, r, on, _, combine)
        if l ≟ Free.point(()) && r ≟ Free.point(()) && on ≟ basicJF =>
      SP.inj(Map(src, combine.void))
    case x => Th.inj(x)
  }

  // TODO write extractor for inject
  def coalesceMap[F[_]: Functor](
    implicit SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => SourcedPathable[T, T[F]] = {
    case x @ Map(Embed(src), mf) => SP.prj(src) match {
      case Some(Map(srcInner, mfInner)) => Map(srcInner, rebase(mf, mfInner))
      case _ => x
    }
    case x => x
  }
}
