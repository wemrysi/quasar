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

import quasar.Predef.{List, String, Vector}
import quasar.{LogicalPlan => LP}
import quasar.contrib.pathy.ADir
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.numeric._
import quasar.sql._
import quasar.std.StdLib.set._

import scala.Option

import matryoshka._, Recursive.ops._
import scalaz._, Leibniz._
import scalaz.std.vector._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.nel._
import scalaz.syntax.writer._

package object quasar {
  type SemanticErrors = NonEmptyList[SemanticError]
  type SemanticErrsT[F[_], A] = EitherT[F, SemanticErrors, A]

  type PhaseResults = Vector[PhaseResult]
  type PhaseResultW[A] = Writer[PhaseResults, A]
  type PhaseResultT[F[_], A] = WriterT[F, PhaseResults, A]

  type CompileM[A] = SemanticErrsT[PhaseResultW, A]

  type EnvErr[A] = Failure[EnvironmentError, A]
  type EnvErrT[F[_], A] = EitherT[F, EnvironmentError, A]

  type PlannerErrT[F[_], A] = EitherT[F, Planner.PlannerError, A]

  private def phase[A: RenderTree](label: String, r: SemanticErrors \/ A):
      CompileM[A] =
      EitherT(r.point[PhaseResultW]) flatMap { a =>
        (a.set(Vector(PhaseResult.tree(label, a)))).liftM[SemanticErrsT]
      }

  /** Compiles a query into raw LogicalPlan, which has not yet been optimized or
    * typechecked.
    */
  // TODO: Move this into the SQL package, provide a type class for it in core.
  def precompile(query: Fix[Sql], vars: Variables, basePath: ADir)(
    implicit RT: RenderTree[Fix[Sql]]):
      CompileM[Fix[LP]] = {
    import SemanticAnalysis.AllPhases

    for {
      ast      <- phase("SQL AST", query.right)
      substAst <- phase("Variables Substituted",
                    Variables.substVars(ast, vars) leftMap (_.wrapNel))
      absAst   <- phase("Absolutized", substAst.mkPathsAbsolute(basePath).right)
      annTree  <- phase("Annotated Tree", AllPhases(absAst))
      logical  <- phase("Logical Plan", Compiler.compile(annTree) leftMap (_.wrapNel))
    } yield logical
  }

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan.
    */
  def preparePlan(lp: Fix[LP]): CompileM[Fix[LP]] =
    for {
      optimized   <- phase("Optimized", Optimizer.optimize(lp).right)
      typechecked <- phase("Typechecked", LP.ensureCorrectTypes(optimized).disjunction)
    } yield typechecked

  /** Identify plans which reduce to a (set of) constant value(s). */
  def refineConstantPlan(lp: Fix[LP]): List[Data] \/ Fix[LP] =
    lp.project match {
      case LP.Constant(Data.Set(records)) => records.left
      case LP.Constant(value)             => List(value).left
      case _                                        => lp.right
    }

  /** Returns the `LogicalPlan` for the given SQL^2 query, or a list of
    * results, if the query was foldable to a constant.
    */
  def queryPlan(
    query: Fix[Sql], vars: Variables, basePath: ADir, off: Natural, lim: Option[Positive]):
      CompileM[List[Data] \/ Fix[LP]] =
    precompile(query, vars, basePath)
      .flatMap(lp => preparePlan(addOffsetLimit(lp, off, lim)))
      .map(refineConstantPlan)

  def addOffsetLimit[T[_[_]]: Corecursive](
    lp: T[LP], off: Natural, lim: Option[Positive]):
      T[LP] = {
    val skipped =
      Drop(lp, LP.Constant[T[LP]](Data.Int(off.get)).embed).embed
    lim.fold(
      skipped)(
      l => Take(skipped, LP.Constant[T[LP]](Data.Int(l.get)).embed).embed)
  }
}
