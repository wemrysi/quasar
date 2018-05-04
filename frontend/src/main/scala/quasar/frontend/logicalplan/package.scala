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

package quasar.frontend

import slamdata.Predef._
import quasar._
import quasar.common.{JoinType, PhaseResult, SortDir}
import quasar.contrib.pathy.FPath
import quasar.namegen.NameGen
import quasar.time.TemporalPart

import scala.Symbol

import matryoshka.data.Fix
import monocle.Prism
import shapeless.Nat
import scalaz._
import scalaz.std.vector._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.writer._

package object logicalplan {
  def read[A] =
    Prism.partial[LogicalPlan[A], FPath] { case Read(p) => p } (Read(_))

  def constant[A] =
    Prism.partial[LogicalPlan[A], Data] { case Constant(p) => p } (Constant(_))

  // TODO this Prism is incorrect
  // it needs to guarantee the same N <: Nat everywhere
  def invoke[A] =
    Prism.partial[LogicalPlan[A], (GenericFunc[Nat], Func.Input[A, Nat])] {
      case Invoke(f, as) => (f, as)
    } ((Invoke[Nat, A](_, _)).tupled)

  def joinSideName[A] =
    Prism.partial[LogicalPlan[A], Symbol] { case JoinSideName(n) => n } (JoinSideName(_))

  def join[A] =
    Prism.partial[LogicalPlan[A], (A, A, JoinType, JoinCondition[A])] {
      case Join(l, r, t, c) => (l, r, t, c)
    } ((Join[A](_, _, _, _)).tupled)

  def free[A] =
    Prism.partial[LogicalPlan[A], Symbol] { case Free(n) => n } (Free(_))

  def let[A] =
    Prism.partial[LogicalPlan[A], (Symbol, A, A)] {
      case Let(v, f, b) => (v, f, b)
    } ((Let[A](_, _, _)).tupled)

  def sort[A] =
    Prism.partial[LogicalPlan[A], (A, NonEmptyList[(A, SortDir)])] {
      case Sort(a, ords) => (a, ords)
    } ((Sort[A](_, _)).tupled)

  def temporalTrunc[A] =
    Prism.partial[LogicalPlan[A], (TemporalPart, A)] {
      case TemporalTrunc(part, a) => (part, a)
    } ((TemporalTrunc[A](_, _)).tupled)

  def typecheck[A] =
    Prism.partial[LogicalPlan[A], (A, Type, A, A)] {
      case Typecheck(e, t, c, f) => (e, t, c, f)
    } ((Typecheck[A](_, _, _, _)).tupled)

  def freshName(prefix: String): State[NameGen, Symbol] =
    quasar.namegen.freshName(prefix).map(Symbol(_))

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan. */
  def preparePlan(lp: Fix[LogicalPlan]): CompileM[Fix[LogicalPlan]] =
    for {
      optimized   <- compilePhase("Optimized", optimizer.optimize(lp).right)
      typechecked <- lpr.ensureCorrectTypes(optimized) flatMap { a =>
        (a.set(Vector(PhaseResult.tree("Typechecked", a)))).liftM[SemanticErrsT]
      }
      rewritten   <- compilePhase("Rewritten Joins", optimizer.rewriteJoins(typechecked).right)
    } yield rewritten

  private val optimizer = new Optimizer[Fix[LogicalPlan]]
  private val lpr = optimizer.lpr
}
