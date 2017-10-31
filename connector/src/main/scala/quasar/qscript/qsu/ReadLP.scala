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

import quasar.{Data, NameGenerator, Planner}, Planner.{NonRepresentableData, PlannerError}
import quasar.contrib.pathy.mkAbsolute
import quasar.contrib.scalaz.MonadError_
import quasar.ejson.EJson
import quasar.frontend.{logicalplan => lp}
import quasar.qscript.MapFuncsCore
import quasar.qscript.qsu.{QScriptUniform => QSU}
import slamdata.Predef.{Map => SMap, _}

import matryoshka.{AlgebraM, BirecursiveT}
import matryoshka.implicits._
import matryoshka.patterns.{interpretM, CoEnv}
import pathy.Path.{rootDir, Sandboxed}
import scalaz.{\/, Monad}
import scalaz.syntax.either._
import scalaz.syntax.monad._

sealed abstract class ReadLP[
    T[_[_]]: BirecursiveT,
    F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator] {

  def apply(plan: T[lp.LogicalPlan]): F[QSUGraph[T]] =
    plan.cataM(transform)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  val transform: AlgebraM[F, lp.LogicalPlan, QSUGraph[T]] = {
    case lp.Read(path) =>
      val afile = mkAbsolute(rootDir[Sandboxed], path)    // TODO this is almost certainly wrong
      withName(QSU.Read(afile))

    case lp.Constant(data) =>
      val back = fromData(data).fold[PlannerError \/ QSU[T, Symbol]](
        {
          case Data.NA => QSU.Nullary(MapFuncsCore.Undefined[T, Symbol]()).right
          case d => NonRepresentableData(d).left
        },
        { x: T[EJson] => QSU.Constant[T, Symbol](x).right })

      MonadError_[F, PlannerError].unattempt(back.point[F]).flatMap(withName)

    case lp.InvokeUnapply(func, values) => ???

    case lp.JoinSideName(name) => ???
    case lp.Join(left, right, tpe, lp.JoinCondition(leftN, rightN, value)) => ???

    case lp.Free(name) => ???
    case lp.Let(name, form, in) => ???

    case lp.Sort(src, order) => ???

    case lp.TemporalTrunc(part, src) => ???

    case lp.Typecheck(expr, tpe, cont, fallback) => ???
  }

  private def withName(node: QSU[T, Symbol]): F[QSUGraph[T]] = {
    for {
      name <- NameGenerator[F].prefixedName("qsu")
      sym = Symbol(name)
    } yield QSUGraph(root = sym, SMap(sym -> node))
  }

  private def fromData(data: Data): Data \/ T[EJson] = {
    data.hyloM[Data \/ ?, CoEnv[Data, EJson, ?], T[EJson]](
      interpretM[Data \/ ?, EJson, Data, T[EJson]](
        _.left,
        _.embed.right),
      Data.toEJson[EJson].apply(_).right)
  }
}

object ReadLP {

  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: MonadError_[?[_], PlannerError]: NameGenerator]: ReadLP[T, F] =
    new ReadLP[T, F] {}
}
