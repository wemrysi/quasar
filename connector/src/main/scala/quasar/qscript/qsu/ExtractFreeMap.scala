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

import slamdata.Predef._

import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.qscript.{JoinFunc, JoinSide, LeftSide, LeftSideF, MFC, RightSide, RightSideF}
import quasar.qscript.MapFuncsCore.{ConcatMaps, MakeMap, StrLit}
import quasar.sql.JoinDir

import matryoshka.CorecursiveT
import scalaz.{Applicative, Free, ValidationNel, Scalaz}, Scalaz._

/** Extracts `MapFunc` expressions from operations by requiring an argument
  * to be a function of one or more sibling arguments and erroring if not.
  *
  * NB: A temporary transformation for prototyping, each of the operations here
  *     will eventually handle arguments having different dimensionality.
  */
object ExtractFreeMap {
  import QScriptUniform._

  def apply[T[_[_]]: CorecursiveT, F[_]: Applicative: PlannerErrorME](graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    QSUGraph.vertices[T].modifyF(_ traverse {
      case GroupBy(src, key) =>
        MappableRegion.unaryOf(src, graph refocus key)
          .map(fm => DimEdit(src, DTrans.Group(fm)))
          .toSuccessNel(s"Invalid group key, $key, must be a mappable function of $src.")

      case LPFilter(src, predicate) =>
        MappableRegion.unaryOf(src, graph refocus predicate)
          .map(QSFilter(src, _))
          .toSuccessNel(s"Invalid filter predicate, $predicate, must be a mappable function of $src.")

      case LPJoin(left, right, cond, jtype, lref, rref) =>
        val combiner: JoinFunc[T] =
          Free.roll(MFC(ConcatMaps(
            Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Left.name), LeftSideF))),
            Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Right.name), RightSideF))))))

        MappableRegion.funcOf(replaceRefs(graph, lref, rref), graph refocus cond)
          .map(ThetaJoin(left, right, _, jtype, combiner))
          .toSuccessNel(s"Invalid join condition, $cond, must be a mappable function of $left and $right.")

      case LPSort(src, keys) =>
        keys traverse { case (k, sortDir) =>
          MappableRegion.unaryOf(src, graph refocus k)
            .strengthR(sortDir)
            .toSuccessNel(s"Invalid sort key, $k, must be a mappable function of $src.")
        } map (QSSort(src, Nil, _))

      case other => other.point[ValidationNel[String, ?]]
    })(graph).fold(e => PlannerErrorME[F].raiseError(InternalError(e intercalate ", ", None)), _.point[F])

  ////

  private def replaceRefs[T[_[_]]](g: QSUGraph[T], l: Symbol, r: Symbol): Symbol => Option[JoinSide] =
    s => g.vertices.get(s) collect {
      case JoinSideRef(`l`) => LeftSide
      case JoinSideRef(`r`) => RightSide
    }
}
