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

import slamdata.Predef._

import quasar.{ParseInstruction => PI}
import quasar.api.resource.ResourcePath
import quasar.common.{CPath, CPathField}
import quasar.connector.{Datasource, MonadResourceErr}
import quasar.ejson.{EJson, Fixed}
import quasar.impl.datasource.{AggregateResult, ChildAggregatingDatasource}
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.{construction, Hole, InterpretedRead, Map, MapFunc, QScriptEducated, RecFreeMap}
import quasar.qscript.RecFreeS._

import scala.util.{Either, Left}

import cats.Monad
import cats.syntax.flatMap._
import cats.syntax.functor._

import fs2.Stream

import matryoshka.data.Fix

import pathy.Path.posixCodec

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

    def sourceFunc = rec.Constant[Hole](EJson.str[Fix[EJson]](posixCodec.printPath(cp.toPath)))

    def reifyPath(path: List[String]): RecFreeMap[Fix] =
      path.foldRight(rec.Hole)(rec.MakeMapS)

    def reifyStructure(
        sourceLoc: Option[List[String]],
        valueLoc: Option[List[String]])
        : RecFreeMap[Fix] =
      (sourceLoc, valueLoc) match {
        case (Some(sloc), Some(vloc)) =>
          rec.ConcatMaps(reifyPath(sloc) >> sourceFunc, reifyPath(vloc))

        case (Some(sloc), None) => reifyPath(sloc) >> sourceFunc
        case (None, Some(vloc)) => reifyPath(vloc)
        case (None, None) => rec.Undefined
      }

    def go(in: List[PI], spath: CPath, vpath: CPath)
        : Option[(List[PI], Option[List[String]], Option[List[String]])] =
      in match {
        case PI.Project(p) :: t if p.hasPrefix(spath) =>
          if (p === spath)
            (t, Some(List()), None)
          else
            (List(), None, None)

        case PI.Project(p) :: t if p.hasPrefix(vpath) =>
          if (p === vpath)
            (t, None, Some(List()))
          else
            (PI.Project(p.dropPrefix(vpath)) :: t, None, Some(List()))

        case PI.Project(_) :: _ =>
          (List(), None, None)

        case PI.Mask(mask) :: t if mask.size === 1 =>
          mask.toList.foldLeftM(List[(CPath, Set[ParseType])]()) {
            case (xs, (p, tpes)) =>
          }

        case PI.Mask(mask) :: t =>

        case other =>
          (other, Some(List(sourceKey)), Some(List(valueKey)))
      }

    ir.instructions match {
      case PI.Ids :: PI.Project(IdIdx) :: _ =>
        (InterpretedRead(cp, ir.instructions), rec.Hole)

      case PI.Ids :: rest =>
        val (out, s, v) = go(rest, ValIdx \ SrcField, ValIdx \ ValField)

        val structure =
          rec.ConcatArrays(
            rec.MakeArray(rec.ProjectIndexI(rec.Hole, 0)),
            rec.MakeArray(reifyStructure(s, v) >> rec.ProjectIndexI(rec.Hole, 1)))

        (InterpretedRead(cp, PI.Ids :: out), structure)

      case rest =>
        val (out, s, v) = go(rest, SrcField, ValField)
        (InterpretedRead(cp, out), reifyStructure(s, v))
    }
  }
}
