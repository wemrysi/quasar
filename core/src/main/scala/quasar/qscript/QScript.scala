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
import quasar.ejson.{Int => _, _}
import quasar.fp._
import quasar.namegen._
import quasar.qscript.MapFunc._
import quasar.qscript.MapFuncs._
import quasar.Planner._
import quasar.Predef._
import quasar.std.StdLib._

import matryoshka._, Recursive.ops._, TraverseT.ops._
import matryoshka.patterns._
import scalaz.{:+: => _, Divide => _, _}, Scalaz._, Inject._, Leibniz._, IndexedStateT._
import shapeless.{Fin, nat, Sized}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - each `Free` structure in a *Join or Union will have exactly one `point`
// - the common source in a Join or Union will be the longest common branch
// - all Reads have a Root (or another Read?) as their source
// - in `Pathable`, the only `MapFunc` node allowed is a `ProjectField`

sealed abstract class JoinType
final case object Inner extends JoinType
final case object FullOuter extends JoinType
final case object LeftOuter extends JoinType
final case object RightOuter extends JoinType

object JoinType {
  implicit val equal: Equal[JoinType] = Equal.equalRef
  implicit val show: Show[JoinType] = Show.showFromToString
}

trait Helpers[T[_[_]]] {
  def equiJF: JoinFunc[T] =
    Free.roll(Eq(Free.point(LeftSide), Free.point(RightSide)))
}

// TODO: Could maybe require only Functor[F], once CoEnv exposes the proper
//       instances
class Transform[T[_[_]]: Recursive: Corecursive: EqualT: ShowT, F[_]: Traverse](
  implicit DE: Const[DeadEnd, ?] :<: F,
           SP: SourcedPathable[T, ?] :<: F,
           QC: QScriptCore[T, ?] :<: F,
           TJ: ThetaJoin[T, ?] :<: F,
           PB: ProjectBucket[T, ?] :<: F,
           // TODO: Remove this one once we have multi-sorted AST
           FI: F :<: QScriptProject[T, ?],
           mergeable:  Mergeable.Aux[T, F],
           eq:         Delay[Equal, F])
    extends Helpers[T] {

  val prov = new Provenance[T]

  type Target[A] = EnvT[Ann[T], F, A]
  type TargetT = Target[T[Target]]

  def DeadEndTarget(deadEnd: DeadEnd): TargetT =
    EnvT[Ann[T], F, T[Target]]((EmptyAnn[T], DE.inj(Const[DeadEnd, T[Target]](deadEnd))))

  val RootTarget: TargetT = DeadEndTarget(Root)
  val EmptyTarget: TargetT = DeadEndTarget(Empty)

  type Envs = List[Target[Unit]]

  case class ZipperSides(
    lSide: FreeMap[T],
    rSide: FreeMap[T])

  case class ZipperTails(
    lTail: Envs,
    rTail: Envs)

  case class ZipperAcc(
    acc: Envs,
    sides: ZipperSides,
    tails: ZipperTails)

  def linearize[F[_]: Functor: Foldable]: Algebra[F, List[F[Unit]]] =
    fl => fl.void :: fl.fold

  def linearizeEnv[E, F[_]: Functor: Foldable]:
      Algebra[EnvT[E, F, ?], List[EnvT[E, F, Unit]]] =
    fl => fl.void :: fl.lower.fold


  def delinearizeInner[F[_]: Functor, A]: Coalgebra[Target, List[Target[A]]] = {
    case Nil => EnvT((EmptyAnn, DE.inj(Const[DeadEnd, List[Target[A]]](Root))))
    case h :: t => h.map(_ => t)
  }

  def delinearizeFreeQS[F[_]: Functor, A]:
      ElgotCoalgebra[Unit \/ ?, Target, List[Target[A]]] = {
    case Nil    => ().left
    case h :: t => h.map(_ => t).right
  }

  val consZipped: Algebra[ListF[Target[Unit], ?], ZipperAcc] = {
    case NilF() => ZipperAcc(Nil, ZipperSides(UnitF[T], UnitF[T]), ZipperTails(Nil, Nil))
    case ConsF(head, ZipperAcc(acc, sides, tails)) => ZipperAcc(head :: acc, sides, tails)
  }

  type FreeEnv = Free[Target, Unit]

  // E, M, F, A => A => M[E[F[A]]]
  val zipper: ElgotCoalgebraM[
      ZipperAcc \/ ?,
      State[NameGen, ?],
      ListF[Target[Unit], ?],
      (ZipperSides, ZipperTails)] = {
    case (zs @ ZipperSides(lm, rm), zt @ ZipperTails(l :: ls, r :: rs)) => {
      mergeable.mergeSrcs(lm, rm, l, r).fold[ZipperAcc \/ ListF[Target[Unit], (ZipperSides, ZipperTails)]]({
          case SrcMerge(inn, lmf, rmf) =>
            ConsF(inn, (ZipperSides(lmf, rmf), ZipperTails(ls, rs))).right[ZipperAcc]
        }, ZipperAcc(Nil, zs, zt).left)
    }
    case (sides, tails) =>
      ZipperAcc(Nil, sides, tails).left.point[State[NameGen, ?]]
  }

  def merge(left: T[Target], right: T[Target]): State[NameGen, SrcMerge[T[Target], Free[Target, Unit]]] = {
    val lLin: Envs = left.cata(linearizeEnv).reverse
    val rLin: Envs = right.cata(linearizeEnv).reverse

    elgotM((
      ZipperSides(UnitF[T], UnitF[T]),
      ZipperTails(lLin, rLin)))(
      consZipped(_: ListF[Target[Unit], ZipperAcc]).point[State[NameGen, ?]], zipper) ∘ {
      case ZipperAcc(common, ZipperSides(lMap, rMap), ZipperTails(lTail, rTail)) =>
        val leftRev =
          foldIso(CoEnv.freeIso[Unit, Target])
            .get(lTail.reverse.ana[T, CoEnv[Unit, Target, ?]](delinearizeFreeQS[F, Unit] >>> (CoEnv(_))))

        val rightRev =
          foldIso(CoEnv.freeIso[Unit, Target])
            .get(rTail.reverse.ana[T, CoEnv[Unit, Target, ?]](delinearizeFreeQS[F, Unit] >>> (CoEnv(_))))

        SrcMerge[T[Target], FreeUnit[Target]](
          common.reverse.ana[T, Target](delinearizeInner),
          rebase(leftRev, Free.roll(EnvT((EmptyAnn, QC.inj(Map(().point[Free[Target, ?]], lMap)))))),
          rebase(rightRev, Free.roll(EnvT((EmptyAnn, QC.inj(Map(().point[Free[Target, ?]], rMap)))))))
    }
  }

  /** This unifies a pair of sources into a single one, with additional
    * expressions to access the combined bucketing info, as well as the left and
    * right values.
    */
  def autojoin(left: T[Target], right: T[Target]):
      (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T]) =
    ??? // TODO

  /** A convenience for a pair of autojoins, does the same thing, but returns
    * access to all three values.
    */
  def autojoin3(left: T[Target], center: T[Target], right: T[Target]):
      (F[T[Target]], List[FreeMap[T]], FreeMap[T], FreeMap[T], FreeMap[T]) = {
    val (lsrc, lbuckets, lval, cval) = autojoin(left, center)
    val (fullSrc, fullBuckets, bval, rval) =
      autojoin(EnvT((Ann[T](lbuckets, UnitF), lsrc)).embed, right)

    (fullSrc, fullBuckets, bval >> lval, bval >> cval, rval)
  }

  def concatBuckets(buckets: List[FreeMap[T]]): (FreeMap[T], List[FreeMap[T]]) =
    (ConcatArraysN(buckets.map(b => Free.roll(MakeArray[T, FreeMap[T]](b))): _*),
      buckets.zipWithIndex.map(p =>
        Free.roll(ProjectIndex[T, FreeMap[T]](
          UnitF[T],
          IntLit[T, Unit](p._2)))))

  def concat[A](l: Free[MapFunc[T, ?], A], r: Free[MapFunc[T, ?], A]):
      State[NameGen, (Free[MapFunc[T, ?], A], Free[MapFunc[T, ?], Unit], Free[MapFunc[T, ?], Unit])] =
    (freshName("lc") ⊛ freshName("rc"))((lname, rname) =>
      (Free.roll(ConcatMaps(
        Free.roll(MakeMap(StrLit[T, A](lname), l)),
        Free.roll(MakeMap(StrLit[T, A](rname), r)))),
        Free.roll(ProjectField(UnitF[T], StrLit[T, Unit](lname))),
        Free.roll(ProjectField(UnitF[T], StrLit[T, Unit](rname)))))

  def merge2Map(
    values: Func.Input[T[Target], nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      State[NameGen, Target[T[Target]]] = {
    val (src, buckets, lval, rval) = autojoin(values(0), values(1))
    val (bucks, newBucks) = concatBuckets(buckets)
    concat(bucks, func(lval, rval).embed) ∘ {
      case (merged, b, v) =>
        EnvT((
          Ann[T](newBucks.map(_ >> b), v),
          // NB: Does it matter what annotation we add to `src` here?
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
    }
  }

  // TODO unify with `merge2Map`
  def merge3Map(
    values: Func.Input[T[Target], nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      State[NameGen, Target[T[Target]]] = {
    val (src, buckets, lval, cval, rval) = autojoin3(values(0), values(1), values(2))
    val (bucks, newBucks) = concatBuckets(buckets)
    concat(bucks, func(lval, cval, rval).embed) ∘ {
      case (merged, b, v) =>
        EnvT((
          Ann[T](newBucks.map(_ >> b), v),
          // NB: Does it matter what annotation we add to `src` here?
          QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))))
    }
  }

  // [1, 2, 3] => [{key: 0, value: 1}, ...]
  // {a: 1, b: 2, c: 3}` => `{a: {key: a, value: 1}, b: {key: b, value: 2}, c: {key: c, value: 3}}`
  def bucketNest(keyName: String, valueName: String): MapFunc[T, FreeMap[T]] = ???

  // TODO namegen
  def shift(input: T[Target]): Target[T[Target]] = ??? // {
  //   val EnvT((Ann[T](provs, vals), src)): Target[T[Target]] =
  //     input.project

  //   val (buck, newBucks) = concatBuckets(provs)
  //   val (merged, buckAccess, valAccess) = concat(buck, vals)

  //   val wrappedSrc: F[T[Target]] =
  //     QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, merged))

  //   Target[T[Target]]((
  //     Ann[T](
  //       newBucks.map(_ >> buckAccess)
  //       vals >> valAccess),
  //     SP.inj(LeftShift(
  //       Target[T[Target]]((EmptyAnn[T], wrappedSrc)).embed,
  //       UnitF[T],
  //       Free.point(RightSide)))))
  // }

  // squash: { squash0: [] }
  // join:   { join1:   {} }

  // def shiftKeys(input: T[Target]): Target[T[Target]] = {
  //   val EnvT((Ann[T](provs, vals), src)): Target[T[Target]] =
  //     input.project

  //   val projValue: FreeMap[T] =
  //     Free.roll(ProjectField[T, FreeMap[T]](UnitF[T], StrLit[T, Unit]("value")))

  //   val wrappedSrc: F[T[Target]] =
  //     QC.inj(Map(EnvT((EmptyAnn[T], src)).embed, Free.roll(DupKeys(UnitF[T]))))

  //   Target[T[Target]]((
  //     Ann[T](
  //       Free.roll(ProjectField[T, FreeMap[T]](UnitF[T], StrLit[T, Unit]("key"))) :: provs,
  //       projValue >> vals),
  //     SP.inj(LeftShift(
  //       Target[T[Target]]((EmptyAnn[T], wrappedSrc)).embed,
  //       projValue,
  //       Free.point(RightSide)))))
  // }

  // NB: More complicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map((), mf), LeftShift((), struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeExpansion1(
    func: UnaryFunc,
    values: Func.Input[T[Target], nat._1]):
      Target[T[Target]] =
    func match {
      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - 12
      //   id(p, x:bar) - 18
      //   id(p, x:foo) - 1
      //   id(p, x:bar) - 2
      // (one bucket)
      case structural.FlattenMap | structural.FlattenArray =>
        shift(values(0)) // TODO

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x:foo) - foo
      //   id(p, x:bar) - bar
      //   id(p, y:foo) - foo
      //   id(p, y:bar) - bar
      // (one bucket)
      case structural.FlattenMapKeys | structural.FlattenArrayIndices =>
        shift(values(0)) // TODO

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - 12
      //   id(p, x, bar) - 18
      //   id(p, y, foo) - 1
      //   id(p, y, bar) - 2
      // (two buckets)
      case structural.ShiftMap | structural.ShiftArray =>
        shift(values(0))

      // id(p, x) - {foo: 12, bar: 18}
      // id(p, y) - {foo: 1, bar: 2}
      //   id(p, x, foo) - foo
      //   id(p, x, bar) - bar
      //   id(p, y, foo) - foo
      //   id(p, y, bar) - bar
      // (two buckets)
      case structural.ShiftMapKeys | structural.ShiftArrayIndices =>
        shift(values(0)) // TODO
    }

  def invokeExpansion2(
    func: BinaryFunc,
    values: Func.Input[T[Target], nat._2]):
      State[NameGen, Target[T[Target]]] =
    func match {
      // TODO left shift `freshName + range value` values onto provenance
      case set.Range =>
        val (src, buckets, lval, rval) = autojoin(values(0), values(1))
        val (bucksArray, newBucks) = concatBuckets(buckets)
        concat(bucksArray, Free.roll(Range(lval, rval))) ∘ {
          case (merged, b, v) =>
            EnvT((
              Ann[T](/*Concat(freshName("range"), v) ::*/ newBucks.map(b >> _), v),
              SP.inj(LeftShift(
                 EnvT((EmptyAnn[T], src)).embed,
                 merged,
                 Free.point[MapFunc[T, ?], JoinSide](RightSide)))))
        }
    }

  def invokeReduction1(
    func: UnaryFunc,
    values: Func.Input[T[Target], nat._1]):
      Target[T[Target]] = {
    val EnvT((Ann(provs, reduce), src)): Target[T[Target]] =
      values(0).project

    val (newProvs, provAccess) = concatBuckets(provs.tail)

    EnvT[Ann[T], F, T[Target]]((
      Ann[T](
        provAccess.map(_ >> Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](0)))),
        Free.roll(ProjectIndex(UnitF[T], IntLit[T, Unit](1)))),
      QC.inj(Reduce[T, T[Target], nat._1](
        EnvT((EmptyAnn[T], src)).embed,
        newProvs,
        Sized[List](
          ReduceFuncs.Arbitrary(newProvs),
          ReduceFunc.translateReduction[FreeMap[T]](func)(reduce)),
        Free.roll(ConcatArrays(
          Free.roll(MakeArray(Free.point(Fin[nat._0, nat._2]))),
          Free.roll(MakeArray(Free.point(Fin[nat._1, nat._2])))))))))
  }

  // TODO: This should definitely be in Matryoshka.
  // apomorphism - short circuit by returning left
  def substitute[T[_[_]], F[_]](original: T[F], replacement: T[F])(implicit T: Equal[T[F]]):
      T[F] => T[F] \/ T[F] =
   tf => if (tf ≟ original) replacement.left else original.right

  // TODO: This should definitely be in Matryoshka.
  def transApoT[T[_[_]]: FunctorT, F[_]: Functor](t: T[F])(f: T[F] => T[F] \/ T[F]):
      T[F] =
    f(t).fold(ι, FunctorT[T].map(_)(_.map(transApoT(_)(f))))

  def invokeThetaJoin(
    values: Func.Input[T[Target], nat._3],
    tpe: JoinType):
      State[NameGen, Target[T[Target]]] = {
    val (src, buckets, lBranch, rBranch, cond) = autojoin3(values(0), values(1), values(2))
    val (buck, newBucks) = concatBuckets(buckets)

    val left: F[Free[F, Unit]] = QC.inj(Map[T, Free[F, Unit]](Free.point[F, Unit](()), lBranch))
    val right: F[Free[F, Unit]] = QC.inj(Map[T, Free[F, Unit]](Free.point[F, Unit](()), rBranch))

    val (concatted, buckAccess, valAccess): (FreeMap[T], FreeMap[T], FreeMap[T]) =
      concat(
        buck: FreeMap[T],
        Free.roll(ConcatMaps(
          Free.roll(MakeMap(StrLit[T, JoinSide]("left"), Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
          Free.roll(MakeMap(StrLit[T, JoinSide]("right"), Free.point[MapFunc[T, ?], JoinSide](RightSide))))))

    state(EnvT((Ann[T](newBucks.map(_ >> buckAccess), valAccess),
      ThetaJoin[T, F[T[Target]]](
        src,
        Free.roll(left).mapSuspension(FI),
        Free.roll(right).mapSuspension(FI),
        cond,
        tpe,
        concatted))))
  }

  def ProjectTarget(prefix: Target[T[Target]], field: FreeMap[T]) = {
    val Ann(provenance, values) = prefix.ask
    EnvT[Ann[T], F, T[Target]]((
      Ann[T](Free.roll(ConcatArrays[T, FreeMap[T]](
        Free.roll(MakeArray[T, FreeMap[T]](UnitF[T])),
        Free.roll(MakeArray[T, FreeMap[T]](field)))) :: provenance, values),
      PB.inj(BucketField(prefix.embed, UnitF[T], field))))
  }

  def pathToProj(path: pathy.Path[_, _, _]): TargetT =
    pathy.Path.peel(path).fold[TargetT](
      RootTarget) {
      case (p, n) =>
        ProjectTarget(pathToProj(p), StrLit(n.fold(_.value, _.value)))
    }

  // TODO error handling
  def fromData[T[_[_]]: Corecursive](data: Data): String \/ T[EJson] = {
    data.hyloM[String \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[String \/ ?, EJson, Data, T[EJson]](
        _.toString.left[T[EJson]],
        _.embed.right[String]),
      Data.toEJson[EJson].apply(_).right)
  }

  def lpToQScript: LogicalPlan[T[Target]] => QSState[TargetT] = {
    case LogicalPlan.ReadF(path) =>
      stateT(pathToProj(path))

    case LogicalPlan.ConstantF(data) =>
      val res = QC.inj(Map(
        RootTarget.embed,
        Free.roll[MapFunc[T, ?], Unit](Nullary[T, FreeMap[T]](fromData(data).fold(
          error => CommonEJson.inj(ejson.Str[T[EJson]](error)).embed,
          ι)))))
      stateT(EnvT((EmptyAnn[T], res)))

    //case LogicalPlan.FreeF(name) =>
    //  val res = QC.inj(Map(
    //    EmptyTarget.embed,
    //    Free.roll(ProjectField(StrLit(name.toString), UnitF[T]))))
    //  stateT(EnvT((EmptyAnn[T], res)))

    //case LogicalPlan.LetF(name, form, body) =>
    //  for {
    //    tmpName <- freshName("let").lift[PlannerError \/ ?]
    //    tup <- merge(form, body).mapK(_.right[PlannerError])
    //    SrcMerge(src, jb1, jb2) = tup
    //    theta <- makeBasicTheta(src, jb1, jb2)
    //  } yield {
    //    QC.inj(Map(
    //      QC.inj(Map(
    //        TJ.inj(theta.src).embed,
    //        Free.roll(ConcatMaps(
    //          Free.roll(MakeMap(StrLit(tmpName), UnitF[T])),
    //          Free.roll(MakeMap(StrLit(name.toString), theta.left)))))).embed,
    //      rebase(theta.right, Free.roll(ProjectField(UnitF[T], StrLit(tmpName))))))
    //  }

    case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      merge3Map(Func.Input3(expr, cont, fallback))(Guard(_, typ, _, _)).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Mapping =>
      val Ann(buckets, value) = a1.project.ask
      val (buck, newBuckets) = concatBuckets(buckets)

      (concat(buck, Free.roll(MapFunc.translateUnaryMapping(func)(UnitF[T]))) ∘ {
        case (mf, bucketAccess, valAccess) =>
          EnvT((
            Ann(newBuckets.map(_ >> bucketAccess), valAccess),
            QC.inj(Map(a1, mf))))}).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(structural.ObjectProject, Sized(a1, a2)) =>
      merge2Map(Func.Input2(a1, a2))(BucketField(_, _)).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(structural.ArrayProject, Sized(a1, a2)) =>
      merge2Map(Func.Input2(a1, a2))(BucketIndex(_, _)).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2))
        if func.effect ≟ Mapping =>
      merge2Map(Func.Input2(a1, a2))(MapFunc.translateBinaryMapping(func)).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))
        if func.effect ≟ Mapping =>
      merge3Map(Func.Input3(a1, a2, a3))(MapFunc.translateTernaryMapping(func)).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1))
        if func.effect ≟ Reduction =>
      invokeReduction1(func, Func.Input1(a1)) map(QC.inj)

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
        case SrcMerge(src, jb1, jb2) =>
          QC.inj(Take(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
      }

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
        case SrcMerge(src, jb1, jb2) =>
          QC.inj(Drop(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
      }

    case LogicalPlan.InvokeFUnapply(set.OrderBy, Sized(a1, a2, a3)) => {
      // we assign extra variables because of:
      // https://issues.scala-lang.org/browse/SI-5589
      // https://issues.scala-lang.org/browse/SI-7515

      (for {
        bucket0 <- findBucket(a1)
        (bucketSrc, bucket, thing) = bucket0
        merged0 <- merge3(a2, a3, bucketSrc)
      } yield {
        val Merge3(src, keys, order, buckets, arrays) = merged0
        val rebasedArrays = rebase(thing, arrays)

        val keysList: List[FreeMap[T]] = rebase(rebasedArrays, keys) match {
          case ConcatArraysN(as) => as
          case mf => List(mf)
        }

        // TODO handle errors
        val orderList: PlannerError \/ List[SortDir] = {
          val orderStrs: PlannerError \/ List[String] = rebase(rebasedArrays, order) match {
            case ConcatArraysN(as) => as.traverse(StrLit.unapply(_)) \/> InternalError("unsupported ordering type") // disjunctionify
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
          orderList.map { keysList.zip(_) }

        (lists.map { pairs =>
          QC.inj(Sort(
            TJ.inj(src).embed,
            rebase(bucket, buckets),
            pairs))
        }).liftM[StateT[?[_], NameGen, ?]]
      }).join
    }

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      mergeTheta[F[Inner]](a1, a2, {
        case SrcMerge(src, fm1, fm2) =>
          QC.inj(Map(
            QC.inj(Filter(TJ.inj(src).embed, fm2)).embed,
            fm1))
      })

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Squashing =>
      val Ann(buckets, value) = a1.project.ask
      val (buck, newBuckets) = concatBuckets(prov.squashProvenances(buckets))
      (concat(buck, value) ∘ { case (mf, buckAccess, valAccess) =>
        EnvT((
          Ann(newBuckets.map(_ >> buckAccess), valAccess),
          QC.inj(Map(a1, mf))))
      }).mapK(_.right[PlannerError])

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect ≟ Expansion =>
      stateT(invokeExpansion1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Expansion =>
      invokeExpansion2(func, Func.Input2(a1, a2)).map(SP.inj)

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect ≟ Transformation =>
      func match {
        case set.GroupBy =>
          mergeTheta[F[Inner]](a1, a2, {
            case SrcMerge(merged, source, bucket) =>
              QB.inj(GroupBy(TJ.inj(merged).embed, source, bucket))
          })
        case set.Union =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              SP.inj(Union(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI)))
          }
        case set.Intersect =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              TJ.inj(ThetaJoin(src, jb1.mapSuspension(FI), jb2.mapSuspension(FI), equiJF, Inner, Free.point(LeftSide)))
          }
        case set.Except =>
          merge(a1, a2).mapK(_.right[PlannerError]) ∘ {
            case SrcMerge(src, jb1, jb2) =>
              TJ.inj(ThetaJoin(
                src,
                jb1.mapSuspension(FI),
                jb2.mapSuspension(FI),
                Free.roll(Nullary(CommonEJson.inj(ejson.Bool[T[EJson]](false)).embed)),
                LeftOuter,
                Free.point(LeftSide)))
          }
      }

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3))  if func.effect ≟ Transformation=>
      def invoke(tpe: JoinType): QSState[F[Inner]] =
        envtHmap(invokeThetaJoin(Func.Input3(a1, a2, a3), tpe))(TJ)

      func match {
        case set.InnerJoin      => invoke(Inner)
        case set.LeftOuterJoin  => invoke(LeftOuter)
        case set.RightOuterJoin => invoke(RightOuter)
        case set.FullOuterJoin  => invoke(FullOuter)
      }

    // TODO Let and FreeVar should not be hit
    case _ => ???
  }
}

class Optimize[T[_[_]]: Recursive: Corecursive: EqualT] extends Helpers[T] {

  // TODO: These optimizations should give rise to various property tests:
  //       • elideNopMap ⇒ no `Map(???, UnitF)`
  //       • normalize ⇒ a whole bunch, based on MapFuncs
  //       • elideNopJoin ⇒ no `ThetaJoin(???, UnitF, UnitF, LeftSide === RightSide, ???, ???)`
  //       • coalesceMaps ⇒ no `Map(Map(???, ???), ???)`
  //       • coalesceMapJoin ⇒ no `Map(ThetaJoin(???, …), ???)`

  // TODO: Turn `elideNop` into a type class?
  // def elideNopFilter[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
  //     QScriptCore[T, T[F]] => F[T[F]] = {
  //   case Filter(src, Patts.True) => src.project
  //   case qc                      => QC.inj(qc)
  // }

  def elideNopMap[F[_]: Functor](implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ UnitF => src.project
    case x                          => QC.inj(x)
  }

  def elideNopJoin[F[_]](
    implicit Th: ThetaJoin[T, ?] :<: F, QC: QScriptCore[T, ?] :<: F):
      ThetaJoin[T, T[F]] => F[T[F]] = {
    case ThetaJoin(src, l, r, on, _, combine)
        if l ≟ Free.point(()) && r ≟ Free.point(()) && on ≟ equiJF =>
      QC.inj(Map(src, combine.void))
    case x => Th.inj(x)
  }

  def simplifyProjections:
      ProjectBucket[T, ?] ~> QScriptCore[T, ?] =
    new (ProjectBucket[T, ?] ~> QScriptCore[T, ?]) {
      def apply[A](proj: ProjectBucket[T, A]) = proj match {
        case BucketField(src, value, field) =>
          Map(src, Free.roll(MapFuncs.ProjectField(value, field)))
        case BucketIndex(src, value, index) =>
          Map(src, Free.roll(MapFuncs.ProjectIndex(value, index)))
      }
    }

  // TODO write extractor for inject
  //SourcedPathable[T, T[CoEnv[A,F, ?]]] => SourcedPathable[T, T[F]] = {
  //F[A] => A  ===> CoEnv[E, F, A] => A
  def coalesceMaps[F[_]: Functor](
    implicit QC: QScriptCore[T, ?] :<: F):
      QScriptCore[T, T[F]] => QScriptCore[T, T[F]] = {
    case x @ Map(Embed(src), mf) => QC.prj(src) match {
      case Some(Map(srcInner, mfInner)) => Map(srcInner, rebase(mf, mfInner))
      case _ => x
    }
    case x => x
  }

  def coalesceMapJoin[F[_]: Functor](
    implicit QC: QScriptCore[T, ?] :<: F, TJ: ThetaJoin[T, ?] :<: F):
      QScriptCore[T, T[F]] => F[T[F]] = {
    case x @ Map(Embed(src), mf) =>
      TJ.prj(src).fold(
        QC.inj(x))(
        tj => TJ.inj(ThetaJoin.combine.modify(mf >> (_: JoinFunc[T]))(tj)))
    case x => QC.inj(x)
  }

  // The order of optimizations is roughly this:
  // - elide NOPs
  // - read conversion given to us by the filesystem
  // - convert any remaning projects to maps
  // - coalesce nodes
  // - normalize mapfunc
  // TODO: Apply this to FreeQS structures.
  def applyAll[F[_]: Functor](
    implicit QC: QScriptCore[T, ?] :<: F,
             TJ: ThetaJoin[T, ?] :<: F,
             PB: ProjectBucket[T, ?] :<: F):
      F[T[F]] => F[T[F]] =
    liftFG(elideNopJoin[F]) ⋙
    liftFG(elideNopMap[F]) ⋙
    quasar.fp.free.injectedNT[F](simplifyProjections) ⋙
    liftFF(coalesceMaps[F]) ⋙
    liftFG(coalesceMapJoin[F]) ⋙
    Normalizable[F].normalize
}
