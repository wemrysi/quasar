/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.qscript.qsu

import quasar.{NameGenerator, Planner}
import quasar.contrib.scalaz.MonadError_
import quasar.frontend.{logicalplan => lp}
import slamdata.Predef._

import matryoshka.{AlgebraM, BirecursiveT}
import matryoshka.implicits._
import scalaz.Monad

object ReadLP {
  import Planner.PlannerError

  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator](
      plan: T[lp.LogicalPlan]): F[QSUGraph[T]] =
    plan.cataM(transform[T, F])

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def transform[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator]
    : AlgebraM[F, lp.LogicalPlan, QSUGraph[T]] = {

    case lp.Read(path) => ???
    case lp.Constant(data) => ???

    case lp.InvokeUnapply(func, values) => ???

    case lp.JoinSideName(name) => ???
    case lp.Join(left, right, tpe, lp.JoinCondition(leftN, rightN, value)) => ???

    case lp.Free(name) => ???
    case lp.Let(name, form, in) => ???

    case lp.Sort(src, order) => ???

    case lp.TemporalTrunc(part, src) => ???

    case lp.Typecheck(expr, tpe, cont, fallback) => ???
  }
}
