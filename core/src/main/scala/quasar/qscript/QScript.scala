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
import quasar.fp._
import quasar.Predef._
import quasar.std.StdLib._

import scala.Predef.implicitly

import matryoshka._, FunctorT.ops._, Recursive.ops._
import matryoshka.patterns._
import pathy.Path._
import scalaz.{:+: => _, _}, Scalaz._, Inject._, Leibniz._
import shapeless.{:: => _, Data => _, Coproduct => _, Const => _, _}

// Need to keep track of our non-type-ensured guarantees:
// - all conditions in a ThetaJoin will refer to both sides of the join
// - each `Free` structure in a *Join or Union will have exactly one `point`
// - the common source in a Join or Union will be the longest common branch
// - all Reads have a Root (or another Read?) as their source
// - in `Pathable`, the only `MapFunc` node allowed is a `ProjectField`

// TODO use real EJson when it lands in master
sealed trait EJson[A]
object EJson {
  def toEJson[T[_[_]]](data: Data): T[EJson] = ???

  final case class Null[A]() extends EJson[A]
  final case class Bool[A](b: Boolean) extends EJson[A]
  final case class Str[A](str: String) extends EJson[A]

  implicit def equal: Delay[Equal, EJson] = new Delay[Equal, EJson] {
    def apply[A](in: Equal[A]): Equal[EJson[A]] = Equal.equal((a, b) => true)
  }

  implicit def functor: Functor[EJson] = new Functor[EJson] {
    def map[A, B](fa: EJson[A])(f: A => B): EJson[B] = fa match {
      case Null() => Null[B]()
      case Bool(b) => Bool[B](b)
      case Str(str) => Str[B](str)
    }
  }

  implicit def show: Delay[Show, EJson] = new Delay[Show, EJson] {
    def apply[A](sh: Show[A]): Show[EJson[A]] = Show.show {
      case Null() => Cord("Null()")
      case Bool(b) => Cord(s"Bool($b)")
      case Str(str) => Cord(s"Str($str)")
    }
  }
}

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

  implicit def mergeable[T[_[_]]]: Mergeable.Aux[T, DeadEnd] = new Mergeable[DeadEnd] {
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
  lBranch: JoinBranch[T],
  rBranch: JoinBranch[T])

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

class Transform[T[_[_]]: Recursive: Corecursive] {
  val mf = new MapFuncs[T, FreeMap[T]]
  import mf._
  val jf = new MapFuncs[T, JoinFunc[T]]

  type Inner = T[QScriptPure[T, ?]]

  type Pures[A] = List[QScriptPure[T, A]]

  val E = implicitly[Const[DeadEnd, ?] :<: QScriptPure[T, ?]]
  val F = implicitly[SourcedPathable[T, ?] :<: QScriptPure[T, ?]]
  val G = implicitly[QScriptCore[T, ?] :<: QScriptPure[T, ?]]
  val H = implicitly[ThetaJoin[T, ?] :<: QScriptPure[T, ?]]

  def linearize[F[_]: Functor: Foldable]: Algebra[F, List[F[Unit]]] =
    fl => fl.void :: fl.fold

  def delinearizeInner[F[_]: Functor, A](implicit DE: Const[DeadEnd, ?] :<: F):
      Coalgebra[F, List[F[A]]] = {
    case Nil    => DE.inj(Const(Root))
    case h :: t => h.map(_ => t)
  }

  def delinearizeJoinBranch[F[_]: Functor, A]:
      ElgotCoalgebra[Unit \/ ?, F, List[F[A]]] = {
    case Nil    => ().left
    case h :: t => h.map(_ => t).right
  }

  type DoublePures = (Pures[Unit], Pures[Unit])
  type DoubleFreeMap = (FreeMap[T], FreeMap[T])
  type TriplePures = (Pures[Unit], Pures[Unit], Pures[Unit])

  val consZipped: Algebra[ListF[QScriptPure[T, Unit], ?], TriplePures] = {
    case NilF() => (Nil, Nil, Nil)
    case ConsF(head, (acc, l, r)) => (head :: acc, l, r)
  }

  val zipper: ElgotCoalgebra[TriplePures \/ ?, ListF[QScriptPure[T, Unit], ?], (DoubleFreeMap, DoublePures)] = {
    case ((_, _), (Nil, Nil)) => (Nil, Nil, Nil).left
    case ((_, _), (Nil, r))   => (Nil, Nil, r).left
    case ((_, _), (l,   Nil)) => (Nil, l,   Nil).left
    case ((lm, rm), (l :: ls, r :: rs)) => {
      val ma = implicitly[Mergeable.Aux[T, QScriptPure[T, Unit]]]

      ma.mergeSrcs(lm, rm, l, r).fold[TriplePures \/ ListF[QScriptPure[T, Unit], (DoubleFreeMap, DoublePures)]](
        (Nil, l :: ls, r :: rs).left) {
        case AbsMerge(inn, lmf, rmf) => ConsF(inn, ((lmf, rmf), (ls, rs))).right[TriplePures]
      }
    }
  }

  def merge(left: Inner, right: Inner): MergeJoin[T, Inner] = {
    val lLin: Pures[Unit] = left.cata(linearize).reverse
    val rLin: Pures[Unit] = right.cata(linearize).reverse

    val (common, lTail, rTail) = ((UnitF[T], UnitF[T]), (lLin, rLin)).elgot(consZipped, zipper)

    AbsMerge[T, Inner, JoinBranch](
      common.reverse.ana[T, QScriptPure[T, ?]](delinearizeInner),
      foldIso(CoEnv.freeIso[Unit, QScriptPure[T, ?]]).get(lTail.reverse.ana[T, CoEnv[Unit, QScriptPure[T, ?], ?]](delinearizeJoinBranch[QScriptPure[T, ?], Unit] >>> (CoEnv(_)))),
      foldIso(CoEnv.freeIso[Unit, QScriptPure[T, ?]]).get(rTail.reverse.ana[T, CoEnv[Unit, QScriptPure[T, ?], ?]](delinearizeJoinBranch[QScriptPure[T, ?], Unit] >>> (CoEnv(_)))))
  }

  def merge2Map(
    values: Func.Input[Inner, nat._2])(
    func: (FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]]):
      SourcedPathable[T, Inner] = {
    val AbsMerge(merged, left, right) = merge(values(0), values(1))
    val res = makeBasicTheta(merged, left, right)

    Map(H.inj(res.src).embed, Free.roll(func(res.left, res.right)))
  }

  def merge3Map(
    values: Func.Input[Inner, nat._3])(
    func: (FreeMap[T], FreeMap[T], FreeMap[T]) => MapFunc[T, FreeMap[T]])(
    implicit ma: Mergeable.Aux[T, QScriptPure[T, Unit]]):
      SourcedPathable[T, Inner] = {

    val AbsMerge(merged, first, second) = merge(values(0), values(1))
    val AbsMerge(merged2, fands, third) = merge(merged, values(2))

    val res = makeBasicTheta(merged2, first, second)
    val res2 = makeBasicTheta(H.inj(res.src).embed, fands, third)

    Map(H.inj(res2.src).embed, Free.roll(func(
      rebase(res2.left, res.left),
      rebase(res2.left, res.right),
      res2.right)))
  }

  def makeBasicTheta[A](src: A, left: JoinBranch[T], right: JoinBranch[T]):
      Merge[T, ThetaJoin[T, A]] =
    AbsMerge[T, ThetaJoin[T, A], FreeMap](
      ThetaJoin(src, left, right, basicJF, Inner,
        Free.roll(jf.ConcatObjects(List(
          Free.roll(jf.MakeObject(Free.roll(jf.StrLit("tmp1")), Free.point[MapFunc[T, ?], JoinSide](LeftSide))),
          Free.roll(jf.MakeObject(Free.roll(jf.StrLit("tmp2")), Free.point[MapFunc[T, ?], JoinSide](RightSide))))))),
      Free.roll(ProjectField(UnitF[T], Free.roll(StrLit("tmp1")))),
      Free.roll(ProjectField(UnitF[T], Free.roll(StrLit("tmp2")))))

  // NB: More compilicated LeftShifts are generated as an optimization:
  // before: ThetaJoin(cs, Map((), mf), LeftShift((), struct, repair), comb)
  // after: LeftShift(cs, struct, comb.flatMap(LeftSide => mf.map(_ => LeftSide), RS => repair))
  def invokeLeftShift(
    func: UnaryFunc,
    values: Func.Input[Inner, nat._1]): SourcedPathable[T, Inner] =
    func match {
      case structural.FlattenMap => LeftShift(values(0), UnitF, Free.point(RightSide))
      case structural.FlattenArray => LeftShift(values(0), UnitF, Free.point(RightSide))
      case structural.ShiftMap => LeftShift(values(0), UnitF, Free.point(RightSide)) // TODO affects bucketing metadata
      case structural.ShiftArray => LeftShift(values(0), UnitF, Free.point(RightSide)) // TODO affects bucketing metadata
      case _ => ???

        // ['a', 'b', 'c'] => [0, 1, 2] // custom map func - does not affect bucketing
        // LeftShift(Map(flattenArrayIndices))
    }

  def translateUnaryMapping[A]: UnaryFunc => A => MapFunc[T, A] = {
    val mf = new MapFuncs[T, A]
    import mf._

    {
      case date.Date => Date(_)
      case date.Time => Time(_)
      case date.Timestamp => Timestamp(_)
      case date.Interval => Interval(_)
      case date.TimeOfDay => TimeOfDay(_)
      case date.ToTimestamp => ToTimestamp(_)
      case math.Negate => Negate(_)
      case relations.Not => Not(_)
      case string.Length => Length(_)
      case string.Lower => Lower(_)
      case string.Upper => Upper(_)
      case string.Boolean => Boolean(_)
      case string.Integer => Integer(_)
      case string.Decimal => Decimal(_)
      case string.Null => Null(_)
      case string.ToString => ToString(_)
      case structural.MakeArray => MakeArray(_)
    }
  }

  def translateBinaryMapping[A]: BinaryFunc => (A, A) => MapFunc[T, A] = {
    val mf = new MapFuncs[T, A]
    import mf._

    {
      // NB: ArrayLength takes 2 params because of SQL, but we really don’t care
      //     about the second. And it shouldn’t even have two in LP.
      case array.ArrayLength => (a, b) => Length(a)
      case date.Extract => Extract(_, _)
      case math.Add      => Add(_, _)
      case math.Multiply => Multiply(_, _)
      case math.Subtract => Subtract(_, _)
      case math.Divide   => Divide(_, _)
      case math.Modulo   => Modulo(_, _)
      case math.Power    => Power(_, _)
      case relations.Eq => Eq(_, _)
      case relations.Neq => Neq(_, _)
      case relations.Lt => Lt(_, _)
      case relations.Lte => Lte(_, _)
      case relations.Gt => Gt(_, _)
      case relations.Gte => Gte(_, _)
      case relations.IfUndefined => IfUndefined(_, _)
      case relations.And => And(_, _)
      case relations.Or => Or(_, _)
      case relations.Coalesce => Coalesce(_, _)
      case set.In => In(_, _)
      case set.Within => Within(_, _)
      case set.Constantly => Constantly(_, _)
      case structural.MakeObject => MakeObject(_, _)
      case structural.ObjectConcat => (l, r) => ConcatObjects(List(l, r))
      case structural.ArrayProject => ProjectIndex(_, _)
      case structural.ObjectProject => ProjectField(_, _)
      case structural.DeleteField => DeleteField(_, _)
      case string.Concat
         | structural.ArrayConcat
         | structural.ConcatOp => (l, r) => ConcatArrays(List(l, r))
    }
  }

  def translateTernaryMapping[A]:
      TernaryFunc => (A, A, A) => MapFunc[T, A] = {
    val mf = new MapFuncs[T, A]
    import mf._

    {
      case relations.Between => Between(_, _, _)
      case relations.Cond    => Cond(_, _, _)
      case string.Like       => Like(_, _, _)
      case string.Search     => Search(_, _, _)
      case string.Substring  => Substring(_, _, _)
    }
  }

  def translateReduction[A]: UnaryFunc => A => ReduceFunc[A] = {
    case agg.Count     => Count(_)
    case agg.Sum       => Sum(_)
    case agg.Min       => Min(_)
    case agg.Max       => Max(_)
    case agg.Avg       => Avg(_)
    case agg.Arbitrary => Arbitrary(_)
  }

  def invokeMapping1[A](func: UnaryFunc, values: Func.Input[A, nat._1]):
      SourcedPathable[T, A] =
    Map(values(0), Free.roll(translateUnaryMapping(func)(UnitF)))

  def invokeMapping2(
    func: BinaryFunc,
    values: Func.Input[Inner, nat._2]): SourcedPathable[T, Inner] =
    merge2Map(values)(translateBinaryMapping(func))

  def invokeMapping3(
    func: TernaryFunc,
    values: Func.Input[Inner, nat._3]): SourcedPathable[T, Inner] =
    merge3Map(values)(translateTernaryMapping(func))

  // TODO we need to handling bucketing from GroupBy
  // the buckets will not always be UnitF, if we have grouped previously
  // GroupBy probably just modifies state
  //
  // TODO handle inner LeftShift
  // an Expansion creates node and modifies state
  //
  // when we get here, look at the bucketing state - consume the state!
  // List[BucketingState] - List[FreeMap]
  // Nil ~ UnitF
  //
  // TODO also we should be able to statically guarantee that we are matching on all reductions here
  // this involves changing how DimensionalEffect is assigned (algebra rather than parameter)
  def invokeReduction1[A](
    func: UnaryFunc, values: Func.Input[A, nat._1]):
      QScriptCore[T, A] =
    Reduce[T, A, nat._0](
      values(0),
      UnitF,
      Sized[List](translateReduction[FreeMap[T]](func)(UnitF)),
      Free.point(Fin[nat._0, nat._1]))

  def basicJF: JoinFunc[T] =
    Free.roll(jf.Eq(Free.point(LeftSide), Free.point(RightSide)))

  def elideNopMaps[F[_]: Functor](
    implicit EqT: Equal[T[EJson]], SP: SourcedPathable[T, ?] :<: F):
      SourcedPathable[T, T[F]] => F[T[F]] = {
    case Map(src, mf) if mf ≟ UnitF => src.project
    case x                          => SP.inj(x)
  }

  def elideNopJoins[F[_]](
    implicit EqT: Equal[T[EJson]],
    Th: ThetaJoin[T, ?] :<: F,
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

  def liftQSAlgebra[F[_], G[_], A](orig: F[A] => G[A])(implicit F: F :<: G):
      G[A] => G[A] =
    ftf => F.prj(ftf).fold(ftf)(orig)

  def liftQSAlgebra2[F[_], G[_], A](orig: F[A] => F[A])(implicit F: F :<: G):
      G[A] => G[A] =
    ftf => F.prj(ftf).fold(ftf)(orig.andThen(F.inj))

  def pathToProj(path: pathy.Path[_, _, _]): FreeMap[T] =
    pathy.Path.peel(path).fold[FreeMap[T]](
      Free.point(())) {
      case (p, n) =>
        Free.roll(ProjectField(pathToProj(p),
          Free.roll(StrLit(n.fold(_.value, _.value)))))
    }

  def lpToQScript: LogicalPlan[Inner] => QScriptPure[T, Inner] = {
    case LogicalPlan.ReadF(path) =>
      F.inj(Map(CorecursiveOps[T, QScriptPure[T, ?]](E.inj(Const[DeadEnd, Inner](Root))).embed, pathToProj(path)))

    case LogicalPlan.ConstantF(data) => F.inj(Map(
      E.inj(Const[DeadEnd, Inner](Root)).embed,
      Free.roll[MapFunc[T, ?], Unit](Nullary[T, FreeMap[T]](EJson.toEJson[T](data)))))

    case LogicalPlan.FreeF(name) => F.inj(Map(
      E.inj(Const[DeadEnd, Inner](Empty)).embed,
      Free.roll(ProjectField(Free.roll(StrLit(name.toString)), UnitF[T]))))

      // case LogicalPlan.TypecheckF(expr, typ, cont, fallback) =>
      //   G.inj(PatternGuard(expr, typ, ???, ???))

    // TODO this illustrates the untypesafe ugliness b/c the pattern match does not guarantee the appropriate sized `Sized`
    // https://github.com/milessabin/shapeless/pull/187
    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Mapping =>
      F.inj(invokeMapping1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) if func.effect == Mapping =>
      F.inj(invokeMapping2(func, Func.Input2(a1, a2)))

    case LogicalPlan.InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2, a3)) if func.effect == Mapping =>
      F.inj(invokeMapping3(func, Func.Input3(a1, a2, a3)))

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Reduction =>
      G.inj(invokeReduction1(func, Func.Input1(a1)))

    case LogicalPlan.InvokeFUnapply(set.Take, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      G.inj(Take(src, jb1, jb2))

    case LogicalPlan.InvokeFUnapply(set.Drop, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)
      G.inj(Drop(src, jb1, jb2))

    case LogicalPlan.InvokeFUnapply(set.Filter, Sized(a1, a2)) =>
      val AbsMerge(src, jb1, jb2) = merge(a1, a2)

      makeBasicTheta(src, jb1, jb2) match {
        case AbsMerge(src, fm1, fm2) =>
          F.inj(Map(G.inj(Filter(H.inj(src).embed, fm2)).embed, fm1))
      }

    case LogicalPlan.InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _, _), Sized(a1)) if func.effect == Expansion =>
      F.inj(invokeLeftShift(func, Func.Input1(a1)))

      //// handling bucketing for sorting
      //// e.g. squashing before a reduce puts everything in the same bucket
      //// TODO consider removing Squashing entirely - and using GroupBy exclusively
      //// Reduce(Sort(LeftShift(GroupBy)))  (LP)
      //// Reduce(Add(GB, GB))
      //// LeftShift and Projection can change grouping/bucketing metadata
      //// y := select sum(pop) (((from zips) group by state) group by substring(city, 0, 2))
      //// z := select sum(pop) from zips group by city
      //case LogicalPlan.InvokeF(func @ UnaryFunc(_, _, _, _, _, _, _, _), input) if func.effect == Squashing => ??? // returning the source with added metadata - mutiple buckets

    case LogicalPlan.InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _, _), Sized(a1, a2)) => {
      func match {
        case set.GroupBy => a1.project // FIXME: add a2 to state
        case set.Union =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          F.inj(Union(src, jb1, jb2))
        case set.Intersect =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          H.inj(ThetaJoin(src, jb1, jb2, basicJF, Inner, Free.point(LeftSide)))
        case set.Except =>
          val AbsMerge(src, jb1, jb2) = merge(a1, a2)
          H.inj(ThetaJoin(src, jb1, jb2, Free.roll(Nullary(EJson.Bool[T[EJson]](false).embed)), LeftOuter, Free.point(LeftSide)))
      }
    }
      //case LogicalPlan.InvokeF(func @ TernaryFunc, input) => {
      //  func match {
      //    // src is Root() - and we rewrite lBranch/rBranch so that () refers to Root()
      //    case InnerJoin => ThetaJoin(input(2), Inner, Root(), input(0), input(1)) // TODO use input(2)
      //    case LeftOuterJoin => ThetaJoin(input(2), LeftOuter, Root(), input(0), input(1)) // TODO use input(2)
      //    case RightOuterJoin => ???
      //    case FullOuterJoin => ???
      //  }
      //}

      //// Map(src=form, MakeObject(name, ()))
      //// Map(Map(src=form, MakeObject(name, ())), body)
      //case LogicalPlan.LetF(name, form, body) => rewriteLet(body)(qsRewrite(name, form))

    case _ => ??? // TODO
  }
}
