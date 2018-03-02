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

package quasar.qscript.qsu

import slamdata.Predef.{Map => SMap, _}

import quasar.{NameGenerator, RenderTreeT}
import quasar.Planner.{InternalError, PlannerErrorME}
import quasar.fp.symbolOrder
import quasar.qscript.{construction, JoinSide, LeftSide, RightSide}
import quasar.sql.JoinDir

import matryoshka.{BirecursiveT, ShowT}
import scalaz.Tags.Disjunction
import scalaz.{Monad, NonEmptyList, IList, Scalaz, StateT, Tag, \/, \/-, -\/, Free, OptionT}, Scalaz._

/** Extracts `MapFunc` expressions from operations by requiring an argument
  * to be a function of one or more sibling arguments and creating an
  * autojoin if not.
  */
final class ExtractFreeMap[T[_[_]]: BirecursiveT: RenderTreeT: ShowT] private () extends QSUTTypes[T] {
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

    case graph @ Extractors.GroupBy(src, key) =>
      unifyShapePreserving[F](graph, src.root, NonEmptyList(key.root))("group_source", "group_key") {
        case (sym, fms) => DimEdit(sym, DTrans.Group(fms.head))
      }

    case graph @ Extractors.LPFilter(src, predicate) =>
      unifyShapePreserving[F](graph, src.root, NonEmptyList(predicate.root))("filter_source", "filter_predicate") {
        case (sym, fms) => QSFilter(sym, fms.head)
      }

    case graph @ Extractors.LPJoin(left, right, cond, jtype, lref, rref) => {
      val graph0 = graph.foldMapUp[IList[(Symbol, Symbol)]](g =>
        g.unfold match {
          case JoinSideRef(`lref`) => IList((g.root, left.root))
          case JoinSideRef(`rref`) => IList((g.root, right.root))
          case _ => IList()
        }).foldLeft(graph) {
          case (g, (src, target)) => g.replace(src, target)
        }

      MappableRegion.funcOf(replaceRefs(graph, lref, rref), graph refocus cond.root).cata(jf =>
        graph.overwriteAtRoot(ThetaJoin(left.root, right.root, jf, jtype, combiner)).point[F], {
          val msg = (desc: String) => InternalError(desc, None)
          val max = MappableRegion.maximal(graph refocus cond.root)
          val nonMappable: IList[QSUGraph] => Option[NonEmptyList[Symbol]] =
            _.filterNot(mappableOf(_, lref, rref)).map(_.root).toNel

          max.toIList.partition(hasJoinRef(_, lref)).bimap(nonMappable(_), nonMappable(_)) match {
            case (Some(lefts), None) =>
              unifyJoin[F](graph0, left.root, lefts, LeftSide, JoinSideRef(lref), JoinSideRef(rref), max)("left_source", "left_target") {
                case (newSrc, on, repair) => ThetaJoin(newSrc, right.root, on, jtype, repair)
              }.getOrElseF(
                PlannerErrorME[F].raiseError[QSUGraph](msg(s"Unable to unify targets: $lefts")))

            case (None, Some(rights)) =>
              unifyJoin[F](graph0, right.root, rights, RightSide, JoinSideRef(lref), JoinSideRef(rref), max)("right_source", "right_target") {
                case (newSrc, on, repair) => ThetaJoin(left.root, newSrc, on, jtype, repair)
              }.getOrElseF(
                PlannerErrorME[F].raiseError[QSUGraph](msg(s"Unable to unify targets: $rights")))

            case (Some(lefts), Some(rights)) => {
              val leftUnify =
                UnifyTargets[T, F](withName[F](_))(graph, left.root, lefts)("left_source", "left_target")
              val rightUnify =
                UnifyTargets[T, F](withName[F](_))(graph, right.root, rights)("right_source", "right_target")

              (leftUnify |@| rightUnify) {
                case ((leftGraph, leftOrig, leftMap0), (rightGraph, rightOrig, rightMap0)) => {
                  val leftMap = SMap(leftMap0.toList: _*)
                  val rightMap = SMap(rightMap0.toList: _*)

                  val on: Option[JoinFunc] = max.traverseM[Option, JoinSide](qg => qg.root match {
                    case sym if leftMap.isDefinedAt(sym) => leftMap.get(sym).map(_.as(LeftSide))
                    case sym if rightMap.isDefinedAt(sym) => rightMap.get(sym).map(_.as(RightSide))
                    case _ => none
                  })

                  val repair: JoinFunc = combiner >>= {
                    case LeftSide => leftOrig.as(LeftSide)
                    case RightSide => rightOrig.as(RightSide)
                  }

                  on.cata(
                    on0 => {
                      val node = ThetaJoin(leftGraph.root, rightGraph.root, on0, jtype, repair)

                      (graph0.overwriteAtRoot(node) :++ leftGraph :++ rightGraph).point[F]
                    }, PlannerErrorME[F].raiseError[QSUGraph](msg(s"Unable to unify targets. Left: $lefts, Right: $rights")))
                }
              }.join
            }
            case _ =>
              PlannerErrorME[F].raiseError[QSUGraph](
                InternalError(s"Invalid join condition, $cond, must be a mappable function of $left and $right.", None))
          }
        })
    }

    case graph @ Extractors.LPSort(src, keys) =>
      unifyShapePreserving[F](graph, src.root, keys map (_._1.root))("sort_source", "sort_key") {
        case (sym, fms) => QSSort(sym, Nil, fms fzip keys.seconds)
      }
  }

  private def combiner: JoinFunc =
    func.StaticMapS(
      JoinDir.Left.name -> func.LeftSide,
      JoinDir.Right.name -> func.RightSide)

  private def hasJoinRef(g: QSUGraph, refId: Symbol): Boolean =
    Tag.unwrap[Boolean, Disjunction](g.foldMapUp(g => g.unfold.map(_.root) match {
      case JoinSideRef(rid) if refId === rid => Tag(true)
      case _ => Tag(false)
    }))

  private def replaceRefs(g: QSUGraph, l: Symbol, r: Symbol)
      : Symbol => Option[JoinSide] =
    s => g.vertices.get(s) collect {
      case JoinSideRef(`l`) => LeftSide
      case JoinSideRef(`r`) => RightSide
    }

  private def partialRefReplace(g: QSUGraph, l: Symbol, r: Symbol): Symbol => JoinSide \/ QSUGraph =
      replaceRefs(g, l, r) >>> (_.toLeft(g).disjunction)

  private def mappableOf(g: QSUGraph, l: Symbol, r: Symbol): Boolean =
      replaceRefs(g, l, r)(g.root).isDefined

  private def unifyShapePreserving[F[_]: Monad: NameGenerator: RevIdxM](
      graph: QSUGraph,
      source: Symbol,
      targets: NonEmptyList[Symbol])(
      sourceName: String,
      targetPrefix: String)(
      buildNode: (Symbol, NonEmptyList[FreeMap]) => QScriptUniform[Symbol]): F[QSUGraph] =
    UnifyTargets[T, F](withName[F](_))(graph, source, targets)(sourceName, targetPrefix) flatMap {
      case (newSrc, original, targetExprs) =>
        val node = buildNode(newSrc.root, targetExprs.seconds)

        if (newSrc.root === source)
          graph.overwriteAtRoot(node).point[F]
        else
          withName[F](node) map { inter =>
            graph.overwriteAtRoot(Map(inter.root, original)) :++ inter :++ newSrc
          }
    }

  private def unifyJoin[F[_]: Monad: NameGenerator: RevIdxM](
      graph: QSUGraph,
      source: Symbol,
      targets: NonEmptyList[Symbol],
      reshapeSide: JoinSide,
      lref: JoinSideRef[T, Symbol],
      rref: JoinSideRef[T, Symbol],
      max: FreeMapA[QSUGraph])(
      sourceName: String,
      targetPrefix: String)(
      buildNode: (Symbol, JoinFunc, JoinFunc) => QScriptUniform[Symbol]): OptionT[F, QSUGraph] =
    UnifyTargets[T, F](withName[F](_))(graph, source, targets)(sourceName, targetPrefix).liftM[OptionT] >>= {
      case (newSrc, original, targetMap) => {
        val repair: JoinFunc = combiner >>= {
          case side if side === reshapeSide => original.as(side)
          case other => Free.pure(other)
        }

        val map = SMap(targetMap.toList: _*)

        OptionT(max.map(qg => partialRefReplace(qg, lref.id, rref.id)(qg.root))
          .traverseM[Option, JoinSide] {
            case -\/(side) => Free.pure(side).some
            case \/-(g) => g.root match {
              case sym if map.isDefinedAt(sym) => map.get(sym).map(_.as(reshapeSide))
              case _ => none
            }
          }.point[F]) >>= { on =>
          val node = buildNode(newSrc.root, on, repair)

          withName[F](node)
            .map(inter => graph.overwriteAtRoot(node) :++ inter :++ newSrc)
            .liftM[OptionT]
        }
      }
    }

  private def withName[F[_]: Monad: NameGenerator: RevIdxM](node: QScriptUniform[Symbol]): F[QSUGraph] =
    QSUGraph.withName[T, F]("efm")(node)
}

object ExtractFreeMap {
  def apply[
      T[_[_]]: BirecursiveT: RenderTreeT: ShowT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (graph: QSUGraph[T])
      : F[QSUGraph[T]] =
    taggedInternalError("ExtractFreeMap", new ExtractFreeMap[T].apply[F](graph))
}
