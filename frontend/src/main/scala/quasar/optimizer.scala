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

package quasar

import scala.Predef.$conforms

import quasar.Predef._
import quasar.{LogicalPlan => LP}
import quasar.contrib.shapeless._
import quasar.fp.binder._
import quasar.namegen._
import quasar.sql.JoinDir
import quasar.fp.ski._

import matryoshka._, Recursive.ops._, FunctorT.ops._, TraverseT.ownOps._
import scalaz._, Scalaz._
import shapeless.{Data => _, :: => _, _}

object Optimizer {
  import LP._
  import quasar.std.StdLib._
  import set._
  import structural._

  private def countUsageƒ(target: Symbol): Algebra[LP, Int] = {
    case Free(symbol) if symbol == target => 1
    case Let(ident, form, _) if ident == target => form
    case x => x.fold
  }

  private def inlineƒ[T[_[_]], A](target: Symbol, repl: LP[T[LP]]):
      LP[(T[LP], T[LP])] => LP[T[LP]] =
  {
    case Free(symbol) if symbol == target => repl
    case Let(ident, form, body) if ident == target =>
      Let(ident, form._2, body._1)
    case x => x.map(_._2)
  }

  private def simplifyƒ[T[_[_]]: Recursive: Corecursive]:
      LP[T[LP]] => Option[LP[T[LP]]] = {
    case inv @ Invoke(func, _) => func.simplify(inv)
    case Let(ident, form, in) => form.project match {
      case Constant(_)
         | Free(_) => in.transPara(inlineƒ(ident, form.project)).project.some
      case _ => in.cata(countUsageƒ(ident)) match {
        case 0 => in.project.some
        case 1 => in.transPara(inlineƒ(ident, form.project)).project.some
        case _ => None
      }
    }
    case _ => None
  }

  def simplifyRepeatedly[T[_[_]]: Recursive: Corecursive]:
      LP[T[LP]] => LP[T[LP]] =
    repeatedly(simplifyƒ[T])

  def simplify[T[_[_]]: Recursive: Corecursive](t: T[LP]): T[LP] =
    t.transCata[LP](simplifyRepeatedly[T])

  /** Like `simplifyƒ`, but eliminates _all_ `Let` (and any bound `Free`) nodes.
    */
  def elideLets[T[_[_]]: Recursive: FunctorT]:
      LP[T[LP]] => Option[LP[T[LP]]] = {
    case Let(ident, form, in) =>
      in.transPara(inlineƒ(ident, form.project)).project.some
    case _ => None
  }

  private val namesƒ: Algebra[LP, Set[Symbol]] = {
    case Free(name) => Set(name)
    case x           => x.fold
  }

  def uniqueName[F[_]: Functor: Foldable](
    prefix: String, plans: F[Fix[LP]]):
      Symbol = {
    val existingNames = plans.map(_.cata(namesƒ)).fold
    def loop(pre: String): Symbol =
      if (existingNames.contains(Symbol(prefix)))
        loop(pre + "_")
      else Symbol(prefix)

    loop(prefix)
  }

  private val shapeƒ: GAlgebra[(Fix[LP], ?), LP, Option[List[Fix[LP]]]] = {
    case Let(_, _, body) => body._2
    case Constant(Data.Obj(map)) =>
      Some(map.keys.map(n => Fix(Constant[Fix[LP]](Data.Str(n)))).toList)
    case InvokeUnapply(DeleteField, Sized(src, field)) =>
      src._2.map(_.filterNot(_ == field._1))
    case InvokeUnapply(MakeObject, Sized(field, _)) => Some(List(field._1))
    case InvokeUnapply(ObjectConcat, srcs) => srcs.traverse(_._2).map(_.flatten)
    // NB: the remaining Invoke cases simply pass through or combine shapes
    //     from their inputs. It would be great if this information could be
    //     handled generically by the type system.
    case InvokeUnapply(OrderBy, Sized(src, _, _)) => src._2
    case InvokeUnapply(Take, Sized(src, _)) => src._2
    case InvokeUnapply(Drop, Sized(src, _)) => src._2
    case InvokeUnapply(Filter, Sized(src, _)) => src._2
    case InvokeUnapply(InnerJoin | LeftOuterJoin | RightOuterJoin | FullOuterJoin, _) =>
      Some(List(JoinDir.Left.const, JoinDir.Right.const))
    case InvokeUnapply(GroupBy, Sized(src, _)) => src._2
    case InvokeUnapply(Distinct, Sized(src, _)) => src._2
    case InvokeUnapply(DistinctBy, Sized(src, _)) => src._2
    case InvokeUnapply(identity.Squash, Sized(src)) => src._2
    case _ => None
  }

  private def preserveFree0[A](x: (Fix[LP], A))(f: A => Fix[LP]):
      Fix[LP] = x._1.unFix match {
    case Free(_) => x._1
    case _        => f(x._2)
  }

  // TODO: implement `preferDeletions` for other backends that may have more
  //       efficient deletes. Even better, a single function that takes a
  //       function parameter deciding which way each case should be converted.
  private val preferProjectionsƒ:
      GAlgebra[
        (Fix[LP], ?),
        LP,
        (Fix[LP], Option[List[Fix[LP]]])] = { node =>

    def preserveFree(x: (Fix[LP], (Fix[LP], Option[List[Fix[LP]]]))) =
      preserveFree0(x)(_._1)

    (node match {
      case InvokeUnapply(DeleteField, Sized(src, field)) =>
        src._2._2.fold(
          Fix(Invoke(DeleteField, Func.Input2(preserveFree(src), preserveFree(field))))) {
          fields =>
            val name = uniqueName("src", fields)
              Fix(Let(name, preserveFree(src),
                Fix(MakeObjectN(fields.filterNot(_ == field._2._1).map(f =>
                  f -> Fix(Invoke(ObjectProject, Func.Input2(Fix(Free[Fix[LP]](name)), f)))): _*))))
        }
      case lp => Fix(lp.map(preserveFree))
    },
      shapeƒ(node.map(_._2)))
  }

  def preferProjections(t: Fix[LP]): Fix[LP] =
    simplify(boundPara(t)(preferProjectionsƒ)._1)

  val elideTypeCheckƒ: Algebra[LP, Fix[LP]] = {
    case Let(n, b, Fix(Typecheck(Fix(Free(nf)), _, cont, _)))
        if n == nf =>
      Fix(Let(n, b, cont))
    case x => Fix(x)
  }

  sealed trait Component[A] {
    def run(l: Fix[LP], r: Fix[LP]): A
  }
  // A condition that refers to left and right sources using equality, so may
  // be rewritten into the join condition:
  final case class EquiCond[A](run0: (Fix[LP], Fix[LP]) => A) extends Component[A] {
    def run(l: Fix[LP], r: Fix[LP]) = run0(l,r)
  }
  // A condition which refers only to the left source:
  final case class LeftCond[A](run0: Fix[LP] => A) extends Component[A] {
    def run(l: Fix[LP], r: Fix[LP]) = run0(l)
  }
  // A condition which refers only to the right source:
  final case class RightCond[A](run0: Fix[LP] => A) extends Component[A] {
    def run(l: Fix[LP], r: Fix[LP]) = run0(r)
  }
  // A condition which refers to both sources but doesn't have the right shape
  // to become the join condition:
  final case class OtherCond[A](run0: (Fix[LP], Fix[LP]) => A) extends Component[A] {
    def run(l: Fix[LP], r: Fix[LP]) = run0(l,r)
  }
  // An expression that doesn't refer to any source.
  final case class NeitherCond[A](run0: A) extends Component[A] {
    def run(l: Fix[LP], r: Fix[LP]) = run0
  }

  // TODO add scalaz propery test
  implicit val ComponentApplicative: Applicative[Component] =
    new Applicative[Component] {
      def point[A](a: => A): Component[A] = NeitherCond(a)

      def ap[A, B](fa: => Component[A])(f: => Component[A => B]): Component[B] =
        (fa, f) match {
          // A             // A => B
          case (NeitherCond(a), NeitherCond(g)) => NeitherCond(g(a))

          // A             // LP => A => B
          case (NeitherCond(a), LeftCond(g))    => LeftCond(g(_)(a))
          // A             // LP => A => B
          case (NeitherCond(a), RightCond(g))   => RightCond(g(_)(a))

          // LP => A       // A => B
          case (LeftCond(a),    NeitherCond(g)) => LeftCond(g <<< a) // lp => g(a(lp))
                                                                     // LP => A       // LP => A => B
          case (LeftCond(a),    LeftCond(g))    => LeftCond(lp => g(lp)(a(lp)))

          // LP => A       // A => B
          case (RightCond(a),   NeitherCond(g)) => RightCond(g <<< a)
          // LP => A       // LP => A => B
          case (RightCond(a),   RightCond(g))   => RightCond(lp => g(lp)(a(lp)))

          case (ca, cg)                         => OtherCond((l, r) => cg.run(l, r)(ca.run(l, r)))
        }
    }

  /** Rewrite joins and subsequent filtering so that:
    * 1) Filtering that is equivalent to an equi-join is rewritten into the join condition.
    * 2) Filtering that refers to only side of the join is hoisted prior to the join.
    * The input plan must have been simplified already so that the structure
    * is in a canonical form for inspection.
    */
  private val rewriteCrossJoinsƒ: LP[(Fix[LP], Fix[LP])] => State[NameGen, Fix[LP]] = { node =>
    import quasar.fp._

    def preserveFree(x: (Fix[LP], Fix[LP])) = preserveFree0(x)(ι)

    def flattenAnd: Fix[LP] => List[Fix[LP]] = {
      case Fix(InvokeUnapply(relations.And, ts)) => ts.unsized.flatMap(flattenAnd)
      case t                                      => List(t)
    }

    def toComp(left: Fix[LP], right: Fix[LP])(c: Fix[LP]):
        Component[Fix[LP]] = {
      c.para[Component[Fix[LP]]] {
        case t if t.map(_._1) ≟ left.unFix  => LeftCond(ι)
        case t if t.map(_._1) ≟ right.unFix => RightCond(ι)

        case InvokeUnapply(relations.Eq, Sized((_, LeftCond(lc)), (_, RightCond(rc)))) =>
          EquiCond((l, r) => Fix(relations.Eq(lc(l), rc(r))))
        case InvokeUnapply(relations.Eq, Sized((_, RightCond(rc)), (_, LeftCond(lc)))) =>
          EquiCond((l, r) => Fix(relations.Eq(rc(r), lc(l))))

        case InvokeUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(t1)) =>
          Func.Input1(t1).map(_._2).sequence[Component, Fix[LP]].map(ts => Fix(Invoke(func, ts)))

        case InvokeUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(t1, t2)) =>
          Func.Input2(t1, t2).map(_._2).sequence[Component, Fix[LP]].map(ts => Fix(Invoke(func, ts)))

        case InvokeUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(t1, t2, t3)) =>
          Func.Input3(t1, t2, t3).map(_._2).sequence[Component, Fix[LP]].map(ts => Fix(Invoke(func, ts)))

        case t => NeitherCond(Fix(t.map(_._1)))
      }
    }

    def assembleCond(conds: List[Fix[LP]]): Fix[LP] =
      conds.foldLeft(Fix(Constant[Fix[LP]](Data.True)))((acc, c) => Fix(relations.And(acc, c)))

    def newJoin(lSrc: Fix[LP], rSrc: Fix[LP], comps: List[Component[Fix[LP]]]):
        State[NameGen, Fix[LP]] = {
      val equis    = comps.collect { case c @ EquiCond(_) => c }
      val lefts    = comps.collect { case c @ LeftCond(_) => c }
      val rights   = comps.collect { case c @ RightCond(_) => c }
      val others   = comps.collect { case c @ OtherCond(_) => c }
      val neithers = comps.collect { case c @ NeitherCond(_) => c }

      for {
        lName  <- freshName("leftSrc")
        lFName <- freshName("left")
        rName  <- freshName("rightSrc")
        rFName <- freshName("right")
        jName  <- freshName("joined")
      } yield {
        // NB: simplifying eagerly to make matching easier up the tree
        simplify(
          Fix(Let(lName, lSrc,
            Fix(Let(lFName, Fix(Filter(Fix(Free(lName)), assembleCond(lefts.map(_.run0(Fix(Free(lName))))))),
              Fix(Let(rName, rSrc,
                Fix(Let(rFName, Fix(Filter(Fix(Free(rName)), assembleCond(rights.map(_.run0(Fix(Free(rName))))))),
                  Fix(Let(jName,
                    Fix(InnerJoin(Fix(Free(lFName)), Fix(Free(rFName)),
                      assembleCond(equis.map(_.run(Fix(Free(lFName)), Fix(Free(rFName))))))),
                    Fix(Filter(Fix(Free(jName)), assembleCond(
                      others.map(_.run0(JoinDir.Left.projectFrom(Fix(Free(jName))), JoinDir.Right.projectFrom(Fix(Free(jName))))) ++
                      neithers.map(_.run0)))))))))))))))
      }
    }


    node match {
      case InvokeUnapply(Filter, Sized((src, Fix(InvokeUnapply(InnerJoin, Sized(joinL, joinR, joinCond)))), (cond, _))) =>
        val comps = flattenAnd(joinCond).map(toComp(joinL, joinR)) ++
                    flattenAnd(cond).map(toComp(JoinDir.Left.projectFrom(src), JoinDir.Right.projectFrom(src)))
        newJoin(joinL, joinR, comps)
      case InvokeUnapply(InnerJoin, Sized((srcL, _), (srcR, _), (_, joinCond))) =>
        newJoin(srcL, srcR, flattenAnd(joinCond).map(toComp(srcL, srcR)))
      case _ => State.state(Fix(node.map(preserveFree)))
    }
  }

  /** Apply universal, type-oblivious transformations intended to
    * improve the performance of a query regardless of the backend. The
    * input is expected to come straight from the SQL^2 compiler or
    * another source of un-optimized queries.
    */
  val optimize: Fix[LP] => Fix[LP] =
    NonEmptyList[Fix[LP] => Fix[LP]](
      // Eliminate extraneous constants, etc.:
      simplify[Fix],

      // NB: must precede normalizeLets to eliminate possibility of shadowing:
      normalizeTempNames,

      // NB: must precede rewriteCrossJoins to normalize Filter/Join shapes:
      normalizeLets,

      // Now for the big one:
      boundParaS(_)(rewriteCrossJoinsƒ).evalZero,

      // Eliminate trivial bindings introduced in rewriteCrossJoins:
      simplify[Fix],

      // Final pass to normalize the resulting plans for better matching in tests:
      normalizeLets,

      // This time, fix the names last so they will read naturally:
      normalizeTempNames

    ).foldLeft1(_ >>> _)
}
