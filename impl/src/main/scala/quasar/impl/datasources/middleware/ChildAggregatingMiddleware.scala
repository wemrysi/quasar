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

import slamdata.Predef.{Map => SMap, _}

import quasar.{FocusedParseInstruction, ParseInstruction => PI, ParseType}
import quasar.api.resource.ResourcePath
import quasar.common.{CPath, CPathField}
import quasar.connector.{Datasource, MonadResourceErr}
import quasar.ejson.{EJson, Fixed}
import quasar.impl.datasource.{AggregateResult, ChildAggregatingDatasource}
import quasar.impl.datasources.ManagedDatasource
import quasar.qscript.{construction, Hole, InterpretedRead, Map, QScriptEducated, RecFreeMap}
import quasar.qscript.RecFreeS._

import scala.util.{Either, Left, Right}

import cats.Monad
import cats.instances.string._
import cats.syntax.eq._
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

    def sourceFunc =
      rec.Constant[Hole](EJson.str[Fix[EJson]](posixCodec.printPath(cp.toPath)))

    def reifyPath(path: List[String]): RecFreeMap[Fix] =
      path.foldRight(rec.Hole)(rec.MakeMapS)

    def dropPrefix(pfx: CPath, from: CPath): CPath =
      from.dropPrefix(pfx) getOrElse from

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

    def injectSource(fields: List[String]): RecFreeMap[Fix] =
      fields.foldLeft(rec.Hole) { (obj, field) =>
        rec.ConcatMaps(obj, rec.MakeMapS(field, sourceFunc))
      }

    /** Returns the rewritten parse instructions and either what sourced-valued fields
      * to add to the output object or whether a new output object should be created
      * having one of, or both, of the source and output value.
      */
    // FIXME: Recursion, need to indicate whether toplevel src/value are included or not
    // FIXME: need to be able to indicate that source needs to be concatenanted for hte cartesian case.
    def go(in: List[PI], spath: CPath, vpath: CPath)
        : (List[PI], Either[List[String], (Option[List[String]], Option[List[String]])]) =
      in match {
        case PI.Project(p) :: t if p.hasPrefix(spath) =>
          if (p === spath)
            (t, Right((Some(List()), None)))
          else
            (List(), Right((None, None)))

        case PI.Project(p) :: t if p.hasPrefix(vpath) =>
          if (p === vpath)
            (t, Right((None, Some(List()))))
          else
            (PI.Project(dropPrefix(vpath, p)) :: t, Right((None, Some(List()))))

        case PI.Project(_) :: _ =>
          (List(), Right((None, None)))

        case PI.Mask(mask) :: t =>
          val exclude: Option[List[String]] = None

          val (mask1, sloc, vloc) =
            mask.foldLeft((SMap[CPath, Set[ParseType]](), exclude, exclude)) {
              case ((m, sp, vp), (k, v)) =>
                if (k === spath && v.contains(ParseType.String))
                  (m, Some(List(sourceKey)), vp)
                else if (k.hasPrefix(vpath))
                  (m.updated(dropPrefix(vpath, k), v), sp, Some(List(valueKey)))
                else
                  (m, sp, vp)
            }

          // FIXME: Have to recurse in case of cartesian/wrap
          (PI.Mask(mask1) :: t, Right((sloc, vloc)))

        // FIXME: Have to recurse in case of cartesian/wrap
        case PI.Wrap(p, n) :: t if p.hasPrefix(spath) =>
          if (p === spath)
            (t, Right((Some(List(sourceKey, n)), Some(List(valueKey)))))
          else
            (t, Right((None, Some(List(valueKey)))))

        // FIXME: Have to recurse in case of cartesian/wrap
        case PI.Wrap(p, n) :: t if p.hasPrefix(vpath) =>
          if (p === vpath)
            (t, Right((Some(List(sourceKey)), Some(List(valueKey, n)))))
          else
            (PI.Wrap(dropPrefix(vpath, p), n) :: t, Right((Some(List(sourceKey)), Some(List(valueKey)))))

        // What if the cartesian includes "source"?
        // I guess we'd need to look at the result path and, instead of making a map with source/value, we'd just concat source to every row, using the name given in the cartesian.
        case PI.Cartesian(cs) :: Nil =>
          val (carteisan, sourceFields) =
            cs.foldLeft((SMap[CPathField, (CPathField, List[FocusedParseInstruction])](), List[String]())) {
              case ((m, srcs), (outField, (inField, cartouche))) if inField.name === sourceKey =>
                (m, inField.name :: srcs)

              case ((m, srcs), assoc @ (outField, (inField, cartouche))) if inField.name === valueKey =>
                (m + assoc, srcs)

              case (res, _) =>
                res
            }

          (cs.isEmpty, sourceFields.isEmpty) match {
            case (true, true) =>
              (Nil, Right((None, None)))

            // no cartesian, just produce a static map.
            case (true, false) =>  ???

            case _ =>
              (List(PI.Wrap(CPath.Identity, valueKey), PI.Cartesian(cs)), Left(sourceFields))
          }

        case other =>
          (other, Right((Some(List(sourceKey)), Some(List(valueKey)))))
      }

    ir.instructions match {
      case PI.Ids :: PI.Project(IdIdx) :: _ =>
        (InterpretedRead(cp, ir.instructions), rec.Hole)

      case PI.Ids :: rest =>
        val (out, struct) = go(rest, ValIdx \ SrcField, ValIdx \ ValField)

        val structure = struct match {
          case Right((s, v)) =>
            rec.ConcatArrays(
              rec.MakeArray(rec.ProjectIndexI(rec.Hole, 0)),
              rec.MakeArray(reifyStructure(s, v) >> rec.ProjectIndexI(rec.Hole, 1)))

          case Left(fields) =>
            injectSource(fields)
        }

        (InterpretedRead(cp, PI.Ids :: out), structure)

      case rest =>
        val (out, struct) = go(rest, SrcField, ValField)
        val structure = struct.fold(injectSource, (reifyStructure _).tupled)
        (InterpretedRead(cp, out), structure)
    }
  }
}
