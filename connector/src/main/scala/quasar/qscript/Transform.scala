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
import quasar.contrib.matryoshka._
import quasar.ejson.{Int => _, _}
import quasar.fp._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._
import quasar.Planner._
import quasar.Predef._
import quasar.std.StdLib._

import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.nonInheritedOps._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._
import shapeless.{nat, Sized}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - the common source in a Join or Union will be the longest common branch
// - any unreferenced src will be `Unreferenced`, and no `Unreferenced` will
//   ever be referenced
// - ReduceIndices will not exceed the reducer bounds, and every reducer will be
//   referenced at least once.

// TODO: Could maybe require only Functor[F], once CoEnv exposes the proper
//       instances
class Transform
  [T[_[_]]: Recursive: Corecursive: FunctorT: EqualT: ShowT,
    F[_]: Traverse: Normalizable]
  (implicit
    C:  Coalesce.Aux[T, F, F],
    DE: Const[DeadEnd, ?] :<: F,
    QC: QScriptCore[T, ?] :<: F,
    TJ: ThetaJoin[T, ?] :<: F,
    PB: ProjectBucket[T, ?] :<: F,
    // TODO: Remove this one once we have multi-sorted AST
    FI: Injectable.Aux[F, QScriptTotal[T, ?]],
    mergeable:  Mergeable.Aux[T, F],
    eq:         Delay[Equal, F],
    show:       Delay[Show, F]) {

  val prov = new Provenance[T]

  type Target = (Ann[T], T[F])
  type LinearF = List[F[ExternallyManaged]]

  case class ZipperSides(
    lSide: FreeMap[T],
    rSide: FreeMap[T])

  case class ZipperTails(
    lTail: LinearF,
    rTail: LinearF)

  case class ZipperAcc(
    acc: LinearF,
    sides: ZipperSides,
    tails: ZipperTails)

  // TODO: Convert to NEL
  def linearize[F[_]: Functor: Foldable]:
      Algebra[F, List[F[ExternallyManaged]]] =
    fl => fl.as[ExternallyManaged](Extern) :: fl.fold

  def delinearizeInner[A]: Coalgebra[F, List[F[A]]] = {
    case Nil    => DE.inj(Const[DeadEnd, List[F[A]]](Root))
    case h :: t => h.as(t)
  }

  def delinearizeTargets[F[_]: Functor, A]:
      ElgotCoalgebra[Hole \/ ?, F, List[F[A]]] = {
    case Nil    => SrcHole.left[F[List[F[A]]]]
    case h :: t => h.as(t).right
  }

  val consZipped: Algebra[ListF[F[ExternallyManaged], ?], ZipperAcc] = {
    case NilF() => ZipperAcc(Nil, ZipperSides(HoleF[T], HoleF[T]), ZipperTails(Nil, Nil))
    case ConsF(head, ZipperAcc(acc, sides, tails)) => ZipperAcc(head :: acc, sides, tails)
  }

  val zipper:
      ElgotCoalgebra[
        ZipperAcc \/ ?,
        ListF[F[ExternallyManaged], ?],
        (ZipperSides, ZipperTails)] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) =>
      mergeable.mergeSrcs(lm, rm, l, r).fold[ZipperAcc \/ ListF[F[ExternallyManaged], (ZipperSides, ZipperTails)]](
        ZipperAcc(Nil, zs, zt).left) {
          case SrcMerge(inn, lmf, rmf) =>
            ConsF(inn, (ZipperSides(lmf, rmf), ZipperTails(ls, rs))).right[ZipperAcc]
      }
    case (sides, tails) =>
      ZipperAcc(Nil, sides, tails).left
  }


  /** Contains a common src, the MapFuncs required to access the left and right
    * sides, and the FreeQS that were unmergeable on either side.
    */
  type MergeResult = (T[F], FreeMap[T], FreeMap[T], Free[F, Hole], Free[F, Hole])

  def merge(left: T[F], right: T[F]): MergeResult = {
    val lLin = left.cata(linearize).reverse
    val rLin = right.cata(linearize).reverse

    val ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =
      elgot(
        (ZipperSides(HoleF[T], HoleF[T]), ZipperTails(lLin, rLin)))(
        consZipped, zipper)

    val leftF =
      foldIso(CoEnv.freeIso[Hole, F])
        .get(lTail.reverse.ana[T, CoEnv[Hole, F, ?]](delinearizeTargets[F, ExternallyManaged] >>> (CoEnv(_))))

    val rightF =
      foldIso(CoEnv.freeIso[Hole, F])
        .get(rTail.reverse.ana[T, CoEnv[Hole, F, ?]](delinearizeTargets[F, ExternallyManaged] >>> (CoEnv(_))))

    val commonSrc: T[F] = common.reverse.ana[T, F](delinearizeInner)

    (commonSrc, lMap, rMap, leftF, rightF)
  }

  def rebaseBranch(br: Free[F, Hole], fm: FreeMap[T]): Free[F, Hole] =
    br >> Free.roll(QC.inj(Map(Free.point[F, Hole](SrcHole), fm)))

  def useMerge(
    left: T[F], right: T[F],
    ap: (T[F], Free[F, Hole], Free[F, Hole]) => (T[F], List[FreeMap[T]], FreeMap[T], FreeMap[T])):
      (T[F], List[FreeMap[T]], FreeMap[T], FreeMap[T]) = {

    val (src, lMap, rMap, lBranch, rBranch) = merge(left, right)

    ap(src, rebaseBranch(lBranch, lMap), rebaseBranch(rBranch, rMap))
  }

  /** This unifies a pair of sources into a single one, with additional
    * expressions to access the combined bucketing info, as well as the left and
    * right values.
    */
  def autojoin(left: Target, right: Target):
      (T[F], List[FreeMap[T]], FreeMap[T], FreeMap[T]) = {
    useMerge(left._2, right._2, (src, lBranch, rBranch) => {
      val lcomp = lBranch.resume
      val rcomp = rBranch.resume
      val (combine, lacc, racc) =
        concat[T, JoinSide](Free.point(LeftSide), Free.point(RightSide))

      val lann = left._1
      val rann = right._1

      // FIXME: Need a better prov representation, to know when the provs are
      //        the same even when the paths to the values differ.
      val commonProv =
        lann.provenance.reverse.zip(rann.provenance.reverse).reverse.foldRightM[List[FreeMap[T]] \/ ?, List[FreeMap[T]]](Nil) {
          case ((l, r), acc) => if (l ≟ r) (l :: acc).right else acc.left
        }.merge

      val commonBuck = concatBuckets(commonProv)

      val condition = commonBuck.fold(
        BoolLit[T, JoinSide](true))( // when both sides are empty, perform a full cross
        c => Free.roll[MapFunc[T, ?], JoinSide](Eq(
          c._1.map(κ(LeftSide)),
          c._1.map(κ(RightSide)))))

      val tj =
        TJ.inj(ThetaJoin(
          src,
          lBranch.mapSuspension(FI.inject),
          rBranch.mapSuspension(FI.inject),
          condition,
          Inner,
          combine)).embed

      (tj,
        prov.joinProvenances(
          lann.provenance.map(_ >> lacc),
          rann.provenance.map(_ >> racc)),
        lann.values >> lacc,
        rann.values >> racc)
    })
  }

  /** A convenience for a pair of autojoins, does the same thing, but returns
    * access to all three values.
    */
  def autojoin3(left: Target, center: Target, right: Target):
      (T[F], List[FreeMap[T]], FreeMap[T], FreeMap[T], FreeMap[T]) = {
    val (lsrc, lbuckets, lval, cval) = autojoin(left, center)
    val (fullSrc, fullBuckets, bval, rval) =
      autojoin((Ann(lbuckets, HoleF), lsrc), right)

    // the holes in `bval` reference `fullSrc`
    // so we replace the holes in `lval` with `bval` because the holes in `lval >> bval` must reference `fullSrc`
    // and `bval` applied to `fullSrc` gives us access to `lsrc`, so we apply `lval` after `bval`
    (fullSrc, fullBuckets, lval >> bval, cval >> bval, rval)
  }

  def merge2Map(
    values: Func.Input[Target, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      Target = {
    val (src, buckets, lval, rval) = autojoin(values(0), values(1))
    concatBuckets(buckets) match {
      case Some((bucks, newBucks)) => {
        val (merged, b, v) = concat(bucks, Free.roll(func(lval, rval)))
        (Ann[T](newBucks.list.toList.map(_ >> b), v),
          QC.inj(Map(src, merged)).embed)
      }
      case None =>
        (EmptyAnn[T], QC.inj(Map(src, Free.roll(func(lval, rval)))).embed)
    }
  }

  // TODO unify with `merge2Map`
  def merge3Map(
    values: Func.Input[Target, nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      Target = {
    val (src, buckets, lval, cval, rval) = autojoin3(values(0), values(1), values(2))
    concatBuckets(buckets) match {
      case Some((bucks, newBucks)) => {
        val (merged, b, v) = concat(bucks, Free.roll(func(lval, cval, rval)))
        (Ann[T](newBucks.list.toList.map(_ >> b), v),
          QC.inj(Map(src, merged)).embed)
      }
      case None =>
        (EmptyAnn[T],
          QC.inj(Map(src, Free.roll(func(lval, cval, rval)))).embed)
    }
  }

  def shiftValues(input: Target, f: FreeMap[T] => MapFunc[T, FreeMap[T]]):
      Target = {
    val Ann(provs, value) = input._1
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc[T, ?], JoinSide](LeftSide),
        Free.point[MapFunc[T, ?], JoinSide](RightSide))

    (Ann[T](
      prov.shiftMap(Free.roll[MapFunc[T, ?], Hole](ProjectIndex(rightAccess, IntLit(0)))) :: provs.map(_ >> leftAccess),
      Free.roll(ProjectIndex(rightAccess, IntLit(1)))),
      QC.inj(LeftShift(input._2, Free.roll(f(value)), sides)).embed)
  }

  def shiftIds(input: Target, f: FreeMap[T] => MapFunc[T, FreeMap[T]]):
      Target = {
    val Ann(provs, value) = input._1
    val (sides, leftAccess, rightAccess) =
      concat(
        Free.point[MapFunc[T, ?], JoinSide](LeftSide),
        Free.point[MapFunc[T, ?], JoinSide](RightSide))

    (Ann(
      prov.shiftMap(rightAccess) :: provs.map(_ >> leftAccess),
      rightAccess),
      QC.inj(LeftShift(input._2, Free.roll(f(value)), sides)).embed)
  }

  def flatten(input: Target): Target = {
    val (Ann(buckets, value), fa) = input
    (Ann(prov.nestProvenances(buckets), value), fa)
  }

  // NB: More complicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map(Hole, mf), LeftShift(Hole, struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(
    func: UnaryFunc,
    values: Func.Input[Target, nat._1]):
      Target =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap =>
        flatten(shiftValues(values(0), ZipMapKeys(_)))
      case structural.FlattenArray =>
        flatten(shiftValues(values(0), ZipArrayIndices(_)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenMapKeys =>
        flatten(shiftIds(values(0), DupMapKeys(_)))
      case structural.FlattenArrayIndices =>
        flatten(shiftIds(values(0), DupArrayIndices(_)))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftMap   => shiftValues(values(0), ZipMapKeys(_))
      case structural.ShiftArray => shiftValues(values(0), ZipArrayIndices(_))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftMapKeys      => shiftIds(values(0), DupMapKeys(_))
      case structural.ShiftArrayIndices => shiftIds(values(0), DupArrayIndices(_))
    }

  def invokeExpansion2(func: BinaryFunc, values: Func.Input[Target, nat._2]):
      Target =
    func match {
      case set.Range =>
        val (src, buckets, lval, rval) = autojoin(values(0), values(1))
        val (sides, leftAccess, rightAccess) =
          concat(
            Free.point[MapFunc[T, ?], JoinSide](LeftSide),
            Free.point[MapFunc[T, ?], JoinSide](RightSide))

        (Ann[T](
          NullLit[T, Hole]() :: buckets.map(_ >> leftAccess),
          rightAccess),
          QC.inj(LeftShift(
            src,
            Free.roll(Range(lval, rval)),
            sides)).embed)
    }

  def invokeReduction1(
    func: UnaryFunc,
    values: Func.Input[Target, nat._1]):
      Target = {
    val Ann(provs, reduce) = values(0)._1
    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold(values(0)) { tail =>
      concatBuckets(tail) match {
        case Some((newProvs, provAccess)) =>
          (Ann[T](
            provAccess.list.toList.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
            Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
            QC.inj(Reduce[T, T[F]](
              values(0)._2,
              newProvs,
              List(
                ReduceFuncs.Arbitrary(newProvs),
                ReduceFunc.translateUnaryReduction[FreeMap[T]](func)(reduce)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
                Free.roll(MakeArray(Free.point(ReduceIndex(1)))))))).embed)
        case None =>
          (EmptyAnn[T],
            QC.inj(Reduce[T, T[F]](
              values(0)._2,
              NullLit(),
              List(ReduceFunc.translateUnaryReduction[FreeMap[T]](func)(reduce)),
              Free.point(ReduceIndex(0)))).embed)
      }
    }
  }

  def invokeReduction2(func: BinaryFunc, values: Func.Input[Target, nat._2])
      : Target = {
    val (src, provs, lMap, rMap) = autojoin(values(0), values(1))

    // NB: If there’s no provenance, then there’s nothing to reduce. We’re
    //     already holding a single value.
    provs.tailOption.fold((EmptyAnn[T], src)) { tail =>
      concatBuckets(tail) match {
        case Some((newProvs, provAccess)) =>
          (Ann[T](
            provAccess.list.toList.map(_ >> Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](0)))),
            Free.roll(ProjectIndex(HoleF[T], IntLit[T, Hole](1)))),
            QC.inj(Reduce[T, T[F]](
              values(0)._2,
              newProvs,
              List(
                ReduceFuncs.Arbitrary(newProvs),
                ReduceFunc.translateBinaryReduction[FreeMap[T]](func)(lMap, rMap)),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(ReduceIndex(0)))),
                Free.roll(MakeArray(Free.point(ReduceIndex(1)))))))).embed)
        case None =>
          (EmptyAnn[T],
            QC.inj(Reduce[T, T[F]](
              values(0)._2,
              NullLit(),
              List(ReduceFunc.translateBinaryReduction[FreeMap[T]](func)(lMap, rMap)),
              Free.point(ReduceIndex(0)))).embed)
      }
    }
  }

  def invokeThetaJoin(values: Func.Input[Target, nat._3], tpe: JoinType)
      : PlannerError \/ Target = {
    val condError: PlannerError \/ JoinFunc[T] = {
      // FIXME: This won’t work where we join a collection against itself
      TJ.prj(values(2)._2.project).fold(
        (InternalError(s"non theta join condition found: ${values(2).shows}"): PlannerError).left[JoinFunc[T]])(
        _.combine.right[PlannerError])
    }

    // NB: This is a magic structure. Improve LP to not imply this structure.
    val combine: JoinFunc[T] = Free.roll(ConcatMaps(
      Free.roll(MakeMap(StrLit[T, JoinSide]("left"), Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
      Free.roll(MakeMap(StrLit[T, JoinSide]("right"), Free.point[MapFunc[T, ?], JoinSide](RightSide)))))

    condError.map { cond =>
      val (commonSrc, lMap, rMap, leftSide, rightSide) = merge(values(0)._2, values(1)._2)
      val Ann(leftBuckets, leftValue) = values(0)._1
      val Ann(rightBuckets, rightValue) = values(1)._1

      val buckets: List[FreeMap[T]] =
        prov.joinProvenances(leftBuckets, rightBuckets)

      // cond >>= {
      //   case LeftSide => leftValue.map(κ(LeftSide))
      //   case RightSide => rightValue.map(κ(RightSide))
      // }

      (Ann[T](buckets, HoleF[T]),
        TJ.inj(ThetaJoin(
          commonSrc,
          rebaseBranch(leftSide, lMap).mapSuspension(FI.inject),
          rebaseBranch(rightSide, rMap).mapSuspension(FI.inject),
          cond,
          tpe,
          combine)).embed)
     }
   }

  def ProjectTarget(prefix: Target, field: FreeMap[T]): Target = {
    val Ann(provenance, values) = prefix._1
    (Ann[T](prov.projectField(field) :: provenance, values),
      PB.inj(BucketField(prefix._2, HoleF[T], field)).embed)
  }

  def pathToProj(path: pathy.Path[_, _, _]): Target =
    pathy.Path.peel(path).fold[Target](
      (EmptyAnn[T], DE.inj(Const[DeadEnd, T[F]](Root)).embed)) {
      case (p, n) =>
        ProjectTarget(pathToProj(p), StrLit(n.fold(_.value, _.value)))
    }

  def fromData[T[_[_]]: Corecursive](data: Data): Data \/ T[EJson] = {
    data.hyloM[Data \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[Data \/ ?, EJson, Data, T[EJson]](
        _.left,
        _.embed.right),
      Data.toEJson[EJson].apply(_).right)
  }

  // TODO: Replace disjunction with validation.
  def lpToQScript: AlgebraM[PlannerError \/ ?, LogicalPlan, (Ann[T], T[F])] = {
    case LogicalPlan.ReadF(path) =>
      // TODO: Compilation of SQL² should insert a ShiftMap at each FROM,
      //       however doing that would break the old Mongo backend, and we can
      //       handle it here for now. But it should be moved to the SQL²
      //       compiler when the old Mongo backend is replaced. (#1298)
      shiftValues(pathToProj(path), ZipMapKeys(_)).right

    case LogicalPlan.ConstantF(data) =>
      fromData(data).fold[PlannerError \/ MapFunc[T, FreeMap[T]]](
        {
          case Data.NA => Undefined[T, FreeMap[T]]().right
          case d       => NonRepresentableData(d).left
        },
        Constant[T, FreeMap[T]](_).right) ∘ (mf =>
        (EmptyAnn[T],
          QC.inj(Map(
            QC.inj(Unreferenced[T, T[F]]()).embed,
            Free.roll[MapFunc[T, ?], Hole](mf))).embed))

    case LogicalPlan.FreeF(name) =>
      (Planner.UnboundVariable(name): PlannerError).left[Target]

    case LogicalPlan.LetF(name, form, body) =>
      (Planner.InternalError("un-elided Let"): PlannerError).left[Target]

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1._1
      concatBuckets(buckets) match {
        case Some((buck, newBuckets)) =>
          val (mf, bucketAccess, valAccess) =
            concat[T, Hole](buck, Free.roll(MapFunc.translateUnaryMapping(func)(HoleF[T])))
          (Ann(newBuckets.list.toList.map(_ >> bucketAccess), valAccess),
            QC.inj(Map(a1._2, mf)).embed).right
        case None =>
          (EmptyAnn[T],
            QC.inj(Map(a1._2, Free.roll(MapFunc.translateUnaryMapping(func)(HoleF[T])))).embed).right
      }

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      (Ann[T](buckets, HoleF[T]),
        PB.inj(BucketField(src, lval, rval)).embed).right

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      (Ann[T](buckets, HoleF[T]),
        PB.inj(BucketIndex(src, lval, rval)).embed).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      merge2Map(Func.Input2(a1, a2))(MapFunc.translateBinaryMapping(func)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      merge3Map(Func.Input3(a1, a2, a3))(MapFunc.translateTernaryMapping(func)).right[PlannerError]

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Reduction =>
      invokeReduction2(func, Func.Input2(a1, a2)).right

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(a1._2, a2._2, (src, lfree, rfree) => {
        (QC.inj(Take(src,
          lfree.mapSuspension(FI.inject),
          rfree.mapSuspension(FI.inject))).embed,
          a1._1.provenance,
          HoleF,
          HoleF)
      })

      (Ann(buckets, HoleF), qs).right

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(a1._2, a2._2, (src, lfree, rfree) => {
        (QC.inj(Drop(src,
          lfree.mapSuspension(FI.inject),
          rfree.mapSuspension(FI.inject))).embed,
          a1._1.provenance,
          HoleF,
          HoleF)
      })

      (Ann(buckets, HoleF), qs).right

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) =>
      val (src, bucketsSrc, ordering, buckets, directions) = autojoin3(a1, a2, a3)

      val bucketsList: List[FreeMap[T]] = buckets.toCoEnv[T].project match {
        case StaticArray(as) => as.map(_.fromCoEnv)
        case mf => List(mf.embed.fromCoEnv)
      }

      val directionsList: PlannerError \/ List[SortDir] = {
        val orderStrs: PlannerError \/ List[String] =
          directions.toCoEnv[T].project match {
            case StaticArray(as) => {
              as.traverse(x => StrLit.unapply(x.project)) \/> InternalError("unsupported ordering type")
            }
            case StrLit(str) => List(str).right
            case _ => InternalError("unsupported ordering function").left
          }
        orderStrs.flatMap {
          _.traverse {
            case "ASC" => SortDir.Ascending.right
            case "DESC" => SortDir.Descending.right
            case _ => InternalError("unsupported ordering direction").left
          }
        }
      }

      val lists: PlannerError \/ List[(FreeMap[T], SortDir)] =
        directionsList.map { bucketsList.zip(_) }

      lists.map(pairs =>
        (Ann[T](bucketsSrc, HoleF[T]),
          QC.inj(Sort(src, ordering, pairs)).embed))

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      (Ann[T](buckets, HoleF[T]),
        QC.inj(Map(QC.inj(Filter(src, rval)).embed, lval)).embed).right

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Squashing =>
      val Ann(buckets, value) = a1._1
      concatBuckets(prov.squashProvenances(buckets)) match {
        case Some((buck, newBuckets)) =>
          val (mf, buckAccess, valAccess) = concat(buck, value)
          (Ann(newBuckets.list.toList.map(_ >> buckAccess), valAccess),
            QC.inj(Map(a1._2, mf)).embed).right
        case None =>
          (EmptyAnn[T],
            QC.inj(Map(a1._2, value)).embed).right
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Expansion =>
      invokeExpansion1(func, Func.Input1(a1)).right

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Expansion =>
      invokeExpansion2(func, Func.Input2(a1, a2)).right

    case LogicalPlan.InvokeFUnapply(set.GroupBy, Sized(a1, a2)) =>
      val (src, buckets, lval, rval) = autojoin(a1, a2)
      (Ann(prov.swapProvenances(rval :: buckets), lval), src).right

    case LogicalPlan.InvokeFUnapply(set.Union, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(a1._2, a2._2, (src, lfree, rfree) => {
        (QC.inj(Union(src,
          lfree.mapSuspension(FI.inject),
          rfree.mapSuspension(FI.inject))).embed,
          prov.unionProvenances(a1._1.provenance, a2._1.provenance),
          HoleF,
          HoleF)
      })

      (Ann(buckets, HoleF), qs).right

    case LogicalPlan.InvokeFUnapply(set.Intersect, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(a1._2, a2._2, (src, lfree, rfree) => {
        (TJ.inj(ThetaJoin(
          src,
          lfree.mapSuspension(FI.inject),
          rfree.mapSuspension(FI.inject),
          EquiJF,
          Inner,
          LeftSideF)).embed,
          prov.joinProvenances(a1._1.provenance, a2._1.provenance),
          HoleF,
          HoleF)
      })

      (Ann(buckets, HoleF), qs).right

    case LogicalPlan.InvokeFUnapply(set.Except, Sized(a1, a2)) =>
      val (qs, buckets, lacc, racc) = useMerge(a1._2, a2._2, (src, lfree, rfree) => {
        (TJ.inj(ThetaJoin(
          src,
          lfree.mapSuspension(FI.inject),
          rfree.mapSuspension(FI.inject),
          BoolLit(false),
          LeftOuter,
          LeftSideF)).embed,
          a1._1.provenance,
          HoleF,
          HoleF)
      })

      (Ann(buckets, HoleF), qs).right

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Transformation =>
      invokeThetaJoin(
        Func.Input3(a1, a2, a3),
        func match {
          case set.InnerJoin      => Inner
          case set.LeftOuterJoin  => LeftOuter
          case set.RightOuterJoin => RightOuter
          case set.FullOuterJoin  => FullOuter
        })
  }
}
