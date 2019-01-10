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

package quasar.impl.datasources.middleware

import slamdata.Predef.{???, String}

import quasar.{ParseInstruction => PI}
import quasar.api.resource.ResourcePath
import quasar.common.{CPath, CPathField}
import quasar.connector.{Datasource, MonadResourceErr}
import quasar.ejson.{EJson, Fixed}
import quasar.impl.datasource.{AggregateResult, ChildAggregatingDatasource}
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.{construction, InterpretedRead, Map, QScriptEducated, RecFreeMap}

import scala.util.{Either, Left}

import cats.Monad
import cats.syntax.functor._

import fs2.Stream

import matryoshka.data.Fix

import shims._

object ChildAggregatingMiddleware {
  def apply[T[_[_]], F[_]: Monad: MonadResourceErr, I, R](
      sourceKey: String,
      valueKey: String)(
      datasourceId: I,
      mds: ManagedDatasource[T, F, Stream[F, ?], R])
      : F[ManagedDatasource[T, F, Stream[F, ?], Either[R, AggregateResult[F, Map[Fix, R]]]]] =
    Monad[F].pure(mds) map {
      case ManagedDatasource.ManagedLightweight(lw) =>
        ManagedDatasource.lightweight[T](
          ChildAggregatingDatasource(lw)(
            _.path,
            rewriteInstructions(sourceKey, valueKey),
            Map[Fix, R](_, _)))

      // TODO: union all in QScript?
      case ManagedDatasource.ManagedHeavyweight(hw) =>
        type Q = T[QScriptEducated[T, ?]]
        ManagedDatasource.heavyweight(
          Datasource.pevaluator[F, Stream[F, ?], Q, R, Q, Either[R, AggregateResult[F, Map[Fix, R]]]]
            .modify(_.map(Left(_)))(hw))
    }

  ////

  private val rec = construction.RecFunc[Fix]
  private val ejs = Fixed[Fix[EJson]]

  private val IdIdx = CPath.parse(".0")
  private val ValIdx = CPath.parse(".1")

  private def rewriteInstructions(
      sourceKey: String,
      valueKey: String)(
      ir: InterpretedRead[ResourcePath],
      cp: ResourcePath)
      : (InterpretedRead[ResourcePath], RecFreeMap[Fix]) = {

    val SrcField = CPath.parse("." + sourceKey)
    val ValField = CPath.parse("." + valueKey)
/*
    def go(
      in: List[ParseInstruction],
      spath: CPath,
      vpath: CPath)
      : (List[ParseInstruction], Option[CPath], Option[CPath]) =

    instrs match {
      case PI.Ids :: PI.Project(IdIdx) :: _ =>
        (instrs, rec.Hole)

      case PI.Ids :: rest =>
        val (out, s, v) = go(rest, ValIdx \ SrcField, ValIdx \ ValField)

      case rest =>
        val (out, s, v) = go(rest, SrcField, ValField)
    }
*/
    ???
  }
}
