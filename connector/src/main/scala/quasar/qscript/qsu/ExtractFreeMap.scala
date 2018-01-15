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
import scalaz.{\/, -\/, \/-, Monad, NonEmptyList => NEL, Scalaz, StateT}, Scalaz._

/** Extracts `MapFunc` expressions from operations by requiring an argument
  * to be a function of one or more sibling arguments and creating an
  * autojoin if not.
  */
final class ExtractFreeMap[T[_[_]]: BirecursiveT] private () extends QSUTTypes[T] {
  import QScriptUniform._
  import QSUGraph.Extractors

  def apply[F[_]: Monad: NameGenerator: PlannerErrorME](graph: QSUGraph)
      : F[QSUGraph] = {
    type G[A] = StateT[F, RevIdx, A]
    graph.rewriteM[G](extract[G]).eval(graph.generateRevIndex)
  }

  ////

  private type QSU[A] = QScriptUniform[A]

  private val func = construction.Func[T]

  private def extract[F[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM]
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

      def autojoinAll(head: Symbol, tail: List[Symbol]): F[QSUGraph] =
        for {
          join <- withName[F](
            AutoJoin2(src.root, head,
              func.StaticMapS(
                "sort_source" -> func.LeftSide,
                head.name -> func.RightSide)))

          updatedG = join :++ graph

          result <- tail.foldLeftM(updatedG) {
            case (g, sym) =>
              val join = withName[F](
                AutoJoin2(g.root, sym,
                  func.ConcatMaps(
                    func.LeftSide,
                    func.MakeMapS(sym.name, func.RightSide))))

              join map (_ :++ g)
          }
        } yield result

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
            joined <- autojoinAll(head, tail)

            order = access.map(_.leftMap {
              case -\/(fm) => fm >> func.ProjectKeyS(func.Hole, "sort_source")
              case \/-(sym) => func.ProjectKeyS(func.Hole, sym.name)
            })

            sort <- withName[F](QSSort(joined.root, Nil, order))

            result = graph.overwriteAtRoot(
              Map(sort.root, func.ProjectKeyS(func.Hole, "sort_source")))

          } yield result :++ sort :++ joined
      }
    }

  private def autojoinFreeMap[F[_]: Monad: NameGenerator: PlannerErrorME: RevIdxM]
    (graph: QSUGraph, src: Symbol, target: Symbol)
    (srcName: String, targetName: String)
    (makeQSU: (Symbol, FreeMap) => QSU[Symbol])
      : F[QSUGraph] =
    MappableRegion.unaryOf(src, graph refocus target)
      .map(makeQSU(src, _)) match {

        case Some(qs) =>
          graph.overwriteAtRoot(qs).point[F]

        case None =>
          val combine: JoinFunc = func.StaticMapS(
            srcName -> func.LeftSide,
            targetName -> func.RightSide)

          for {
            join <- withName[F](AutoJoin2(src, target, combine))
            inter <- withName[F](makeQSU(join.root, func.ProjectKeyS(func.Hole, targetName)))
            result = graph.overwriteAtRoot(Map(inter.root, func.ProjectKeyS(func.Hole, srcName)))

          } yield result :++ inter :++ join
      }

  private def replaceRefs(g: QSUGraph, l: Symbol, r: Symbol)
      : Symbol => Option[JoinSide] =
    s => g.vertices.get(s) collect {
      case JoinSideRef(`l`) => LeftSide
      case JoinSideRef(`r`) => RightSide
    }

  private def withName[F[_]: Monad: NameGenerator: RevIdxM](node: QScriptUniform[Symbol]): F[QSUGraph] =
    QSUGraph.withName[T, F]("extractfm")(node)
}

object ExtractFreeMap {
  def apply[
      T[_[_]]: BirecursiveT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    taggedInternalError("ExtractFreeMap", new ExtractFreeMap[T].apply[F](graph))
}
