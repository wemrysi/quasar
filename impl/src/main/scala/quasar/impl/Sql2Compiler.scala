/*
 * Copyright 2020 Precog Data
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

package quasar.impl

import slamdata.Predef.{Long, None, StringContext}

import quasar.RenderTreeT
import quasar.common.{phaseM, PhaseResultTell}
import quasar.compile.{queryPlan, MonadSemanticErrs}
import quasar.contrib.cats.stateT._
import quasar.contrib.iota._
import quasar.ejson.implicits._
import quasar.fp._
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.impl.implicits._
import quasar.qscript.{MonadPlannerErr, QScriptEducated}
import quasar.qsu.LPtoQS
import quasar.sql.{parser, MonadParsingErr}

import cats.Monad
import cats.data.{Kleisli, StateT}
import cats.syntax.flatMap._
import cats.syntax.functor._

import matryoshka._

import org.slf4s.Logging

import pathy.Path.posixCodec

import shims.monadToScalaz

object Sql2Compiler extends Logging {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      F[_]: Monad: MonadQuasarErr: PhaseResultTell]
      : Kleisli[F, SqlQuery, T[QScriptEducated[T, ?]]] =
    Kleisli(sql2ToQScript[T, F]) local { squery =>
      log.debug(s"Evaluating query=${squery.query.value}, basePath=${posixCodec.printPath(squery.basePath)}")
      squery
    }

  def sql2ToQScript[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      F[_]: Monad: MonadParsingErr: MonadPlannerErr: MonadSemanticErrs: PhaseResultTell](
      sqlQuery: SqlQuery)
      : F[T[QScriptEducated[T, ?]]] =
    for {
      sql <- MonadParsingErr[F].unattempt_(parser[T].parseExpr(sqlQuery.query.value))

      lp  <- queryPlan[F, T, T[LP]](sql, sqlQuery.vars, sqlQuery.basePath, 0, None)

      qs  <- phaseM[F]("QScript (Educated)", LPtoQS[T].apply[StateT[F, Long, ?]](lp).runA(0))
    } yield qs
}
