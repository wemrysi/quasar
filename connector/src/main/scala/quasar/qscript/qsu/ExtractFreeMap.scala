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

import quasar.NameGenerator
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.common.SortDir
import quasar.qscript.{construction, JoinSide, LeftSide, RightSide}
import quasar.sql.JoinDir

import matryoshka.BirecursiveT
import scalaz.{\/, -\/, \/-, Applicative, Functor, Monad, NonEmptyList => NEL, Scalaz}, Scalaz._

/** Extracts `MapFunc` expressions from operations by requiring an argument
  * to be a function of one or more sibling arguments and creating an
  * autojoin if not.
  */
final class ExtractFreeMap[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QScriptUniform._
  import QSUGraph.Extractors

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](graph: QSUGraph)
      : F[QSUGraph] =
    graph.rewriteM[F](extract[F])

  ////

  private type QSU[A] = QScriptUniform[A]

  private val func = construction.Func[T]

  private def extract[F[_]: Monad: NameGenerator: PlannerErrorME]
      : PartialFunction[QSUGraph, F[QSUGraph]] = {

    // This will only work once #3170 is completed.
    // We need access to the group key through the `Map`.
    case graph @ Extractors.GroupBy(src, key) =>
      autojoinFreeMap[F](graph, src.root, key.root)("group_source", "group_key") {
        case (sym, fm) => DimEdit(sym, DTrans.Group(fm))
      }

    case graph @ Extractors.LPFilter(src, predicate) =>
      autojoinFreeMap[F](graph, src.root, predicate.root)("filter_source", "filter_predicate") {
        case (sym, fm) => QSFilter(sym, fm)
      }

    case graph @ Extractors.LPJoin(left, right, cond, jtype, lref, rref) =>
      val combiner: JoinFunc =
        func.StaticMapS(
          JoinDir.Left.name -> func.LeftSide,
          JoinDir.Right.name -> func.RightSide)

      MappableRegion.funcOf(replaceRefs(graph, lref, rref), graph refocus cond.root)
        .map(jf => ThetaJoin(left.root, right.root, jf, jtype, combiner)) match {
          case Some(qs) =>
            graph.overwriteAtRoot(qs).point[F]
          case None =>
            PlannerErrorME[F].raiseError[QSUGraph](
              InternalError(s"Invalid join condition, $cond, must be a mappable function of $left and $right.", None))
        }

    case graph @ Extractors.LPSort(src, keys) =>
      val access: NEL[(FreeMap \/ Symbol, SortDir)] =
        keys.map(_.leftMap { key =>
          MappableRegion.unaryOf(src.root, graph refocus key.root) match {
            case Some(fm) => fm.left[Symbol]
            case None => key.root.right[FreeMap]
          }
        })

      val nonmappable: List[Symbol] = access.toList collect {
        case ((\/-(sym), _)) => sym
      }

      def autojoinVerts(head: Symbol, tail: List[Symbol]): F[(Symbol, QSUVerts[T])] = {
        freshName[F].flatMap { joinRoot =>
          val join: QSU[Symbol] = AutoJoin2(src.root, head,
            func.StaticMapS("sort_source" -> func.LeftSide, head.name -> func.RightSide))

	      val updated: QSUVerts[T] = graph.vertices.updated(joinRoot, join)

          tail.foldLeftM((joinRoot, updated)) {
            case ((prev, verts), sym) =>
              freshName[F].map { innerRoot =>
                val join: QSU[Symbol] =
	              AutoJoin2(prev, sym, func.ConcatMaps(func.LeftSide, func.MakeMapS(sym.name, func.RightSide)))

	            (innerRoot, verts.updated(innerRoot, join))
              }
          }
        }
      }

      nonmappable match {
        case Nil =>
          access.toList collect {
            case ((-\/(fm), dir)) => (fm, dir)
          } match {
            case Nil =>
              PlannerErrorME[F].raiseError[QSUGraph](InternalError(s"No sort keys found.", None))
            case head :: tail =>
              graph.overwriteAtRoot(QSSort(src.root, Nil, NEL(head, tail: _*))).point[F]
          }

        case head :: tail =>
          for {
            interRoot <- freshName[F]
            joined <- autojoinVerts(head, tail)
          } yield {
            val (joinRoot, verts) = joined

            val order: NEL[(FreeMap, SortDir)] =
              access.map(_.leftMap {
                case -\/(fm) => fm >> func.ProjectKeyS(func.Hole, "sort_source")
                case \/-(sym) => func.ProjectKeyS(func.Hole, sym.name)
              })

            val sort: QSU[Symbol] = QSSort(joinRoot, Nil, order)
            val result: QSU[Symbol] = Map(interRoot, func.ProjectKeyS(func.Hole, "sort_source"))

            val newVerts: QSUVerts[T] = verts
              .updated(interRoot, sort)
              .updated(graph.root, result)

            QSUGraph(graph.root, newVerts)
          }
      }
    }

  private def autojoinFreeMap[F[_]: Applicative: NameGenerator: PlannerErrorME]
    (graph: QSUGraph, src: Symbol, target: Symbol)
    (srcName: String, targetName: String)
    (makeQSU: (Symbol, FreeMap) => QSU[Symbol])
      : F[QSUGraph] =
    MappableRegion.unaryOf(src, graph refocus target)
      .map(makeQSU(src, _)) match {

        case Some(qs) => graph.overwriteAtRoot(qs).point[F]

        case None => (freshName[F] |@| freshName[F]) {
          case (joinRoot, interRoot) =>
            val combine: JoinFunc = func.StaticMapS(
              srcName -> func.LeftSide,
              targetName -> func.RightSide)

            val join: QSU[Symbol] = AutoJoin2(src, target, combine)
            val inter: QSU[Symbol] = makeQSU(joinRoot, func.ProjectKeyS(func.Hole, targetName))
            val result: QSU[Symbol] = Map(interRoot, func.ProjectKeyS(func.Hole, srcName))

            val QSUGraph(origRoot, origVerts) = graph

            val newVerts: QSUVerts[T] = origVerts
              .updated(joinRoot, join)
              .updated(interRoot, inter)
              .updated(origRoot, result)

            QSUGraph(origRoot, newVerts)
        }
      }

  private def replaceRefs(g: QSUGraph, l: Symbol, r: Symbol)
      : Symbol => Option[JoinSide] =
    s => g.vertices.get(s) collect {
      case JoinSideRef(`l`) => LeftSide
      case JoinSideRef(`r`) => RightSide
    }

  private def freshName[F[_]: Functor: NameGenerator]: F[Symbol] =
    freshSymbol("extract")
}

object ExtractFreeMap {
  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    taggedInternalError("ExtractFreeMap", new ExtractFreeMap[T].apply[F](graph))
}
