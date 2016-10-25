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
import quasar.contrib.pathy.{FPath, refineTypeAbs}
import quasar.contrib.shapeless._
import quasar.fp._
import quasar.fp.ski._
import quasar.fp.binder._
import quasar.namegen._

import scala.Some

import matryoshka._, Recursive.ops._, FunctorT.ops._
import scalaz._, Scalaz._, Validation.success, Validation.FlatMap._
import shapeless.{:: => _, Id => _, _}
import pathy.Path.posixCodec

sealed trait LogicalPlan[A] extends Product with Serializable
object LogicalPlan {
  import quasar.std.StdLib._
  import structural._

  implicit val LogicalPlanTraverse: Traverse[LogicalPlan] =
    new Traverse[LogicalPlan] {
      def traverseImpl[G[_], A, B](
        fa: LogicalPlan[A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[LogicalPlan[B]] =
        fa match {
          case ReadF(coll)           => G.point(ReadF(coll))
          case Constant(data)       => G.point(Constant(data))
          case InvokeF(func, values) => values.traverse(f).map(InvokeF(func, _))
          case Free(v)              => G.point(Free(v))
          case Let(ident, form, in) => (f(form) ⊛ f(in))(Let(ident, _, _))
          case Typecheck(expr, typ, cont, fallback) =>
            (f(expr) ⊛ f(cont) ⊛ f(fallback))(Typecheck(_, typ, _, _))
        }

      override def map[A, B](v: LogicalPlan[A])(f: A => B): LogicalPlan[B] =
        v match {
          case ReadF(coll)           => ReadF(coll)
          case Constant(data)       => Constant(data)
          case InvokeF(func, values) => InvokeF(func, values.map(f))
          case Free(v)              => Free(v)
          case Let(ident, form, in) => Let(ident, f(form), f(in))
          case Typecheck(expr, typ, cont, fallback) =>
            Typecheck(f(expr), typ, f(cont), f(fallback))
        }

      override def foldMap[A, B](fa: LogicalPlan[A])(f: A => B)(implicit B: Monoid[B]): B =
        fa match {
          case ReadF(_)              => B.zero
          case Constant(_)          => B.zero
          case InvokeF(_, values)    => values.foldMap(f)
          case Free(_)              => B.zero
          case Let(_, form, in)     => f(form) ⊹ f(in)
          case Typecheck(expr, _, cont, fallback) =>
            f(expr) ⊹ f(cont) ⊹ f(fallback)
        }

      override def foldRight[A, B](fa: LogicalPlan[A], z: => B)(f: (A, => B) => B): B =
        fa match {
          case ReadF(_)              => z
          case Constant(_)          => z
          case InvokeF(_, values)    => values.foldRight(z)(f)
          case Free(_)              => z
          case Let(ident, form, in) => f(form, f(in, z))
          case Typecheck(expr, _, cont, fallback) =>
            f(expr, f(cont, f(fallback, z)))
        }
    }
  implicit val RenderTreeLogicalPlan:
      Delay[RenderTree, LogicalPlan] =
    new Delay[RenderTree, LogicalPlan] {
      def apply[A](ra: RenderTree[A]): RenderTree[LogicalPlan[A]] =
        new RenderTree[LogicalPlan[A]] {
          val nodeType = "LogicalPlan" :: Nil

          def render(v: LogicalPlan[A]) = v match {
            // NB: a couple of special cases for readability
            case Constant(Data.Str(str)) => Terminal("Str" :: "Constant" :: nodeType, Some(str.shows))
            case InvokeFUnapply(func @ structural.ObjectProject, Sized(expr, name)) =>
              (ra.render(expr), ra.render(name)) match {
                case (RenderedTree(_, Some(x), Nil), RenderedTree(_, Some(n), Nil)) =>
                  Terminal("ObjectProject" :: nodeType, Some(x + "{" + n + "}"))
                case (x, n) => NonTerminal("Invoke" :: nodeType, Some(func.shows), x :: n :: Nil)
              }

            case ReadF(file)                => Terminal("Read" :: nodeType, Some(posixCodec.printPath(file)))
            case Constant(data)            => Terminal("Constant" :: nodeType, Some(data.shows))
            case InvokeFUnapply(func, args) => NonTerminal("Invoke" :: nodeType, Some(func.shows), args.unsized.map(ra.render))
            case Free(name)                => Terminal("Free" :: nodeType, Some(name.toString))
            case Let(ident, form, body)    => NonTerminal("Let" :: nodeType, Some(ident.toString), List(ra.render(form), ra.render(body)))
            case Typecheck(expr, typ, cont, fallback) =>
              NonTerminal("Typecheck" :: nodeType, Some(typ.shows),
                List(ra.render(expr), ra.render(cont), ra.render(fallback)))
          }
        }
    }

  implicit val EqualFLogicalPlan: EqualF[LogicalPlan] =
    new EqualF[LogicalPlan] {
      def equal[A: Equal](v1: LogicalPlan[A], v2: LogicalPlan[A]): Boolean =
        (v1, v2) match {
          case (ReadF(n1), ReadF(n2)) => refineTypeAbs(n1) ≟ refineTypeAbs(n2)
          case (Constant(d1), Constant(d2)) => d1 ≟ d2
          case (InvokeFUnapply(f1, v1), InvokeFUnapply(f2, v2)) => f1 == f2 && v1.unsized ≟ v2.unsized
          case (Free(n1), Free(n2)) => n1 ≟ n2
          case (Let(ident1, form1, in1), Let(ident2, form2, in2)) =>
            ident1 ≟ ident2 && form1 ≟ form2 && in1 ≟ in2
          case (Typecheck(expr1, typ1, cont1, fb1), Typecheck(expr2, typ2, cont2, fb2)) =>
            expr1 ≟ expr2 && typ1 ≟ typ2 && cont1 ≟ cont2 && fb1 ≟ fb2
          case _ => false
        }
    }

  final case class ReadF[A](path: FPath) extends LogicalPlan[A] {
    override def toString = s"""Read("${path.shows}")"""
  }
  object Read {
    def apply(path: FPath): Fix[LogicalPlan] =
      Fix[LogicalPlan](new ReadF(path))
  }

  final case class Constant[A](data: Data) extends LogicalPlan[A]

  final case class InvokeF[A, N <: Nat](func: GenericFunc[N], values: Func.Input[A, N]) extends LogicalPlan[A] {
    override def toString = {
      func.shows + "(" + values.mkString(", ") + ")"
    }
    override def equals(that: scala.Any): Boolean = that match {
      case that @ InvokeF(_, _) =>
        (this.func == that.func) && (this.values.unsized == that.values.unsized)
      case _ => false
    }
  }
  // TODO we create a custom `unapply` to bypass a scalac pattern matching bug
  // https://issues.scala-lang.org/browse/SI-5900
  object InvokeFUnapply {
    def unapply[A, N <: Nat](in: InvokeF[A, N]): Some[(GenericFunc[N], Func.Input[A, N])] = Some((in.func, in.values))
  }
  object Invoke {
    def apply[N <: Nat](func: GenericFunc[N], values: Func.Input[Fix[LogicalPlan], N]): Fix[LogicalPlan] =
      Fix[LogicalPlan](InvokeF(func, values))
  }

  final case class Free[A](name: Symbol) extends LogicalPlan[A]

  final case class Let[A](let: Symbol, form: A, in: A) extends LogicalPlan[A]

  // NB: This should only be inserted by the type checker. In future, this
  //     should only exist in BlackShield – the checker will annotate nodes
  //     where runtime checks are necessary, then they will be added during
  //     compilation to BlackShield.
  final case class Typecheck[A](expr: A, typ: Type, cont: A, fallback: A) extends LogicalPlan[A]

  implicit val LogicalPlanUnzip: Unzip[LogicalPlan] = new Unzip[LogicalPlan] {
    def unzip[A, B](f: LogicalPlan[(A, B)]) = (f.map(_._1), f.map(_._2))
  }

  implicit val LogicalPlanBinder: Binder[LogicalPlan] = new Binder[LogicalPlan] {
      type G[A] = Map[Symbol, A]
      val G = Traverse[G]

      def initial[A] = Map[Symbol, A]()

      def bindings[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A])(f: LogicalPlan[T[LogicalPlan]] => A): G[A] =
        t match {
          case Let(ident, form, _) => b + (ident -> f(form.project))
          case _                   => b
        }

      def subst[T[_[_]]: Recursive, A](t: LogicalPlan[T[LogicalPlan]], b: G[A]): Option[A] =
        t match {
          case Free(symbol) => b.get(symbol)
          case _             => None
        }
    }

  def freshName(prefix: String): State[NameGen, Symbol] =
    quasar.namegen.freshName(prefix).map(Symbol(_))

  // NB: this can't currently be generalized to Binder, because the key type isn't exposed there.
  def renameƒ[M[_]: Monad](f: Symbol => M[Symbol]):
      ((Map[Symbol, Symbol], Fix[LogicalPlan])) =>
        M[LogicalPlan[(Map[Symbol, Symbol], Fix[LogicalPlan])]] =
  { case (bound, t) =>
    t.unFix match {
      case Let(sym, expr, body) =>
        f(sym).map(sym1 =>
          Let(sym1,
            (bound, expr),
            (bound + (sym -> sym1), body)))
      case Free(sym) =>
        val v: LogicalPlan[(Map[Symbol, Symbol], Fix[LogicalPlan])] =
          Free(bound.get(sym).getOrElse(sym))
        v.point[M]
      case t =>
        t.strengthL(bound).point[M]
    }
  }

  def rename[M[_]: Monad](f: Symbol => M[Symbol])(t: Fix[LogicalPlan]): M[Fix[LogicalPlan]] =
    (Map[Symbol, Symbol](), t).anaM(renameƒ(f))

  def normalizeTempNames(t: Fix[LogicalPlan]) =
    rename[State[NameGen, ?]](κ(freshName("tmp")))(t).evalZero

  /** Per the following:
    * 1. Successive Lets are re-associated to the right:
    *    (let a = (let b = x1 in x2) in x3) becomes
    *    (let b = x1 in (let a = x2 in x3))
    * 2. Lets are "hoisted" outside of Invoke and Typecheck nodes:
    *    (add (let a = x1 in x2) (let b = x3 in x4)) becomes
    *    (let a = x1 in (let b = x3 in (add x2 x4))
    * Note that this is safe only if all bound names are unique; otherwise
    * it could create spurious shadowing. normalizeTempNames is recommended.
    * NB: at the moment, Lets are only hoisted one level.
    */
  val normalizeLetsƒ: LogicalPlan[Fix[LogicalPlan]] => Option[LogicalPlan[Fix[LogicalPlan]]] = {
      case Let(b, Fix(Let(a, x1, x2)), x3) => Let(a, x1, Fix(Let(b, x2, x3))).some

      // TODO generalize the following three `GenericFunc` cases
      case InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) => a1 match {
        case Fix(Let(a, x1, x2)) => Let(a, x1, Invoke[nat._1](func, Func.Input1(x2))).some
        case _ => None
      }

      case InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) => (a1, a2) match {
        case (Fix(Let(a, x1, x2)), a2) => Let(a, x1, Invoke[nat._2](func, Func.Input2(x2, a2))).some
        case (a1, Fix(Let(a, x1, x2))) => Let(a, x1, Invoke[nat._2](func, Func.Input2(a1, x2))).some
        case _ => None
      }

      case InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) => (a1, a2, a3) match {
        case (Fix(Let(a, x1, x2)), a2, a3) => Let(a, x1, Invoke[nat._3](func, Func.Input3(x2, a2, a3))).some
        case (a1, Fix(Let(a, x1, x2)), a3) => Let(a, x1, Invoke[nat._3](func, Func.Input3(a1, x2, a3))).some
        case (a1, a2, Fix(Let(a, x1, x2))) => Let(a, x1, Invoke[nat._3](func, Func.Input3(a1, a2, x2))).some
        case _ => None
      }

      case Typecheck(Fix(Let(a, x1, x2)), typ, cont, fallback) =>
        Let(a, x1, Fix(Typecheck(x2, typ, cont, fallback))).some
      case Typecheck(expr, typ, Fix(Let(a, x1, x2)), fallback) =>
        Let(a, x1, Fix(Typecheck(expr, typ, x2, fallback))).some
      case Typecheck(expr, typ, cont, Fix(Let(a, x1, x2))) =>
        Let(a, x1, Fix(Typecheck(expr, typ, cont, x2))).some

      case t => None
  }

  def normalizeLets(t: Fix[LogicalPlan]) = t.transAna(repeatedly(normalizeLetsƒ))

  final case class NamedConstraint(name: Symbol, inferred: Type, term: Fix[LogicalPlan])
  final case class ConstrainedPlan(inferred: Type, constraints: List[NamedConstraint], plan: Fix[LogicalPlan])

  type Typed[F[_]] = Cofree[F, Type]
  type SemValidation[A] = ValidationNel[SemanticError, A]
  type SemDisj[A] = NonEmptyList[SemanticError] \/ A

  def inferTypes(typ: Type, term: Fix[LogicalPlan]):
      SemValidation[Typed[LogicalPlan]] = {

    (term.unFix match {
      case ReadF(c) =>
        success(ReadF[Typed[LogicalPlan]](c))

      case Constant(d) =>
        success(Constant[Typed[LogicalPlan]](d))

      case InvokeF(func, args) => for {
        types <- func.untpe(typ)
        args0 <- types.zip(args).traverse {
          case (t, arg) => inferTypes(t, arg)
        }
      } yield InvokeF(func, args0)

      case Free(n) => success(Free[Typed[LogicalPlan]](n))

      case Let(n, form, in) =>
        inferTypes(typ, in).flatMap { in0 =>
          val fTyp = in0.collect {
            case Cofree(typ0, Free(n0)) if n0 == n => typ0
          }.concatenate(Type.TypeGlbMonoid)
          inferTypes(fTyp, form).map(Let[Typed[LogicalPlan]](n, _, in0))
        }

      case Typecheck(expr, t, cont, fallback) =>
        (inferTypes(t, expr) ⊛ inferTypes(typ, cont) ⊛ inferTypes(typ, fallback))(
          Typecheck[Typed[LogicalPlan]](_, t, _, _))

    }).map(Cofree(typ, _))
  }

  private def lift[A](v: SemDisj[A]): NameT[SemDisj, A] =
    quasar.namegen.lift[SemDisj](v)

  /** This function compares the inferred (required) type with the possible type
    * from the collection.
    * • if it’s a const type, replace the node with a constant
    * • if the possible is a subtype of the inferred, we’re good
    * • if the inferred is a subtype of the possible, we need a runtime check
    * • otherwise, we fail
    */
  private def unifyOrCheck(inf: Type, poss: Type, term: Fix[LogicalPlan]):
      NameT[SemDisj, ConstrainedPlan] = {
    if (inf.contains(poss))
      emit(ConstrainedPlan(poss, Nil, poss match {
        case Type.Const(d) => Fix(Constant(d))
        case _ => term
      }))
    else if (poss.contains(inf)) {
      emitName(freshName("check").map(name =>
        ConstrainedPlan(inf, List(NamedConstraint(name, inf, term)), Fix(Free(name)))))
    }
    else lift((SemanticError.genericError(s"You provided a ${poss.shows} where we expected a ${inf.shows} in $term")).wrapNel.left)
  }

  private def appConst(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]):
      Fix[LogicalPlan] =
    constraints.constraints.foldLeft(constraints.plan)((acc, con) =>
      Fix(Let(con.name, con.term,
        Fix(Typecheck(Fix(Free(con.name)), con.inferred, acc, fallback)))))

  /** This inserts a constraint on a node that might not strictly require a type
    * check. It protects operations (EG, array flattening) that need a certain
    * shape.
    */
  private def ensureConstraint(constraints: ConstrainedPlan, fallback: Fix[LogicalPlan]): State[NameGen, Fix[LogicalPlan]] = {
    val ConstrainedPlan(typ, consts, term) = constraints
      (consts match {
        case Nil =>
          freshName("check").map(name =>
            ConstrainedPlan(typ, List(NamedConstraint(name, typ, term)), Fix(Free(name))))
        case _   => constraints.point[State[NameGen, ?]]
      }).map(appConst(_, fallback))
  }

  // TODO: This can perhaps be decomposed into separate folds for annotating
  //       with “found” types, folding constants, and adding runtime checks.
  val checkTypesƒ:
      ((Type, LogicalPlan[ConstrainedPlan])) => NameT[SemDisj, ConstrainedPlan] = {
    case (inf, term) =>
      def applyConstraints(
          poss: Type, constraints: ConstrainedPlan)
          (f: Fix[LogicalPlan] => Fix[LogicalPlan]) =
        unifyOrCheck(inf, poss, f(appConst(constraints, Fix(Constant(Data.NA)))))

      term match {
        case ReadF(c)         => unifyOrCheck(inf, Type.Top, Read(c))
        case Constant(d)     => unifyOrCheck(inf, Type.Const(d), Fix(Constant(d)))
        case InvokeFUnapply(MakeObject, Sized(name, value)) =>
          lift(MakeObject.tpe(Func.Input2(name, value).map(_.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(x => Fix(MakeObject(name.plan, x))))
        case InvokeFUnapply(MakeArray, Sized(value)) =>
          lift(MakeArray.tpe(Func.Input1(value).map(_.inferred)).disjunction).flatMap(
            applyConstraints(_, value)(x => Fix(MakeArray(x))))
        // TODO: Move this case to the Mongo planner once type information is
        //       available there.
        case InvokeFUnapply(ConcatOp, Sized(left, right)) =>
          val (types, constraints0, terms) = Func.Input2(left, right).map {
            case ConstrainedPlan(in, con, pl) => (in, (con, pl))
          }.unzip3[Type, List[NamedConstraint], Fix[LogicalPlan]]

          val constraints = constraints0.unsized.flatten

          lift(ConcatOp.tpe(types).disjunction).flatMap[NameGen, ConstrainedPlan](poss => poss match {
            case t if Type.Str.contains(t) => unifyOrCheck(inf, poss, Invoke(string.Concat, terms))
            case t if t.arrayLike => unifyOrCheck(inf, poss, Invoke(ArrayConcat, terms))
            case _                => lift(-\/(NonEmptyList(SemanticError.GenericError("can't concat mixed/unknown types"))))
          }).map(cp =>
            cp.copy(constraints = cp.constraints ++ constraints))
        case InvokeFUnapply(relations.Or, Sized(left, right)) =>
          lift(relations.Or.tpe(Func.Input2(left, right).map(_.inferred)).disjunction).flatMap(unifyOrCheck(inf, _, Invoke(relations.Or, Func.Input2(left, right).map(appConst(_, Fix(Constant(Data.NA)))))))
        case InvokeFUnapply(structural.FlattenArray, Sized(arg)) =>
          for {
            types <- lift(structural.FlattenArray.tpe(Func.Input1(arg).map(_.inferred)).disjunction)
            consts <- emitName[SemDisj, Func.Input[Fix[LogicalPlan], nat._1]](Func.Input1(arg).traverse(ensureConstraint(_, Fix(Constant(Data.Arr(List(Data.NA)))))))
            plan  <- unifyOrCheck(inf, types, Invoke[nat._1](structural.FlattenArray, consts))
          } yield plan
        case InvokeFUnapply(structural.FlattenMap, Sized(arg)) => for {
          types <- lift(structural.FlattenMap.tpe(Func.Input1(arg).map(_.inferred)).disjunction)
          consts <- emitName[SemDisj, Func.Input[Fix[LogicalPlan], nat._1]](Func.Input1(arg).traverse(ensureConstraint(_, Fix(Constant(Data.Obj(ListMap("" -> Data.NA)))))))
          plan  <- unifyOrCheck(inf, types, Invoke[nat._1](structural.FlattenMap, consts))
        } yield plan
        case InvokeFUnapply(func @ UnaryFunc(_, _, _, _, _, _, _), Sized(a1)) =>
          handleGenericInvoke(inf, InvokeF(func, Func.Input1(a1)))
        case InvokeFUnapply(func @ BinaryFunc(_, _, _, _, _, _, _), Sized(a1, a2)) =>
          handleGenericInvoke(inf, InvokeF(func, Func.Input2(a1, a2)))
        case InvokeFUnapply(func @ TernaryFunc(_, _, _, _, _, _, _), Sized(a1, a2, a3)) =>
          handleGenericInvoke(inf, InvokeF(func, Func.Input3(a1, a2, a3)))
        case Typecheck(expr, typ, cont, fallback) =>
          unifyOrCheck(inf, Type.glb(cont.inferred, typ), Fix(Typecheck(expr.plan, typ, cont.plan, fallback.plan)))
        case Let(name, value, in) =>
          unifyOrCheck(inf, in.inferred, Fix(Let(name, appConst(value, Fix(Constant(Data.NA))), appConst(in, Fix(Constant(Data.NA))))))
        // TODO: Get the possible type from the Let
        case Free(v) => emit(ConstrainedPlan(inf, Nil, Fix(Free(v))))
      }
  }

  private def handleGenericInvoke[N <: Nat](inf: Type, invoke: InvokeF[ConstrainedPlan, N]): NameT[SemDisj, ConstrainedPlan] = {
    val func: GenericFunc[N] = invoke.func
    val args: Func.Input[ConstrainedPlan, N] = invoke.values

    func.effect match {
      case Mapping =>
        val (types, constraints0, terms) = args.map {
          case ConstrainedPlan(in, con, pl) => (in, (con, pl))
        }.unzip3[Type, List[NamedConstraint], Fix[LogicalPlan]]

        val constraints = constraints0.unsized.flatten

        lift(func.tpe(types).disjunction).flatMap(
          unifyOrCheck(inf, _, Invoke(func, terms))).map(cp =>
            cp.copy(constraints = cp.constraints ++ constraints))

      case _ =>
        lift(func.tpe(args.map(_.inferred)).disjunction).flatMap(
          unifyOrCheck(inf, _, Invoke(func, args.map(appConst(_, Fix(Constant(Data.NA)))))))
    }
  }

  type SemNames[A] = NameT[SemDisj, A]

  def ensureCorrectTypes(term: Fix[LogicalPlan]):
      ValidationNel[SemanticError, Fix[LogicalPlan]] = {
    // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
    import StateT.stateTMonadState

    inferTypes(Type.Top, term).flatMap(
      cofCataM[LogicalPlan, SemNames, Type, ConstrainedPlan](_)(checkTypesƒ).map(appConst(_, Fix(Constant(Data.NA)))).evalZero.validation)
  }

  // TODO: Generalize this to Binder
  def lpParaZygoHistoM[M[_]: Monad, A, B](
    t: Fix[LogicalPlan])(
    f: LogicalPlan[(Fix[LogicalPlan], B)] => B,
    g: LogicalPlan[Cofree[LogicalPlan, (B, A)]] => M[A]):
      M[A] = {
    def loop(t: Fix[LogicalPlan], bind: Map[Symbol, Cofree[LogicalPlan, (B, A)]]):
        M[Cofree[LogicalPlan, (B, A)]] = {
      lazy val default: M[Cofree[LogicalPlan, (B, A)]] = for {
        lp <- t.unFix.traverse(x => for {
          co <- loop(x, bind)
        } yield ((x, co.head._1), co))
        (xb, co) = lp.unfzip
        b = f(xb)
        a <- g(co)
      } yield Cofree((b, a), co)

      t.unFix match {
        case Free(name)            => bind.get(name).fold(default)(_.point[M])
        case Let(name, form, body) => for {
          form1 <- loop(form, bind)
          rez   <- loop(body, bind + (name -> form1))
        } yield rez
        case _                      => default
      }
    }

    for {
      rez <- loop(t, Map())
    } yield rez.head._2
  }

  def lpParaZygoHistoS[S, A, B] = lpParaZygoHistoM[State[S, ?], A, B] _
  def lpParaZygoHisto[A, B] = lpParaZygoHistoM[Id, A, B] _

  /** The set of paths referenced in the given plan. */
  def paths(lp: Fix[LogicalPlan]): Set[FPath] =
    lp.foldMap(_.cata[Set[FPath]] {
      case ReadF(p) => Set(p)
      case other    => other.fold
    })
}
