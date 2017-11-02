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

import quasar.contrib.pathy.AFile
import quasar.qscript.{
  Filter,
  JoinSide,
  LeftShift,
  LeftSideF,
  Map,
  MFC,
  QCE,
  Read,
  Reduce,
  RightSideF,
  Sort,
  SrcMerge,
  Subset,
  ThetaJoin,
  TTypes,
  Union}
import quasar.qscript.MapFuncsCore.{ConcatMaps, MakeMap, StrLit}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.QSUGraph.QSUPattern
import quasar.sql.JoinDir

import matryoshka.{Corecursive, CorecursiveT, Coalgebra, Recursive}
import scalaz.{Const, Free, Inject}

sealed abstract class Graduate[T[_[_]]: CorecursiveT] extends TTypes[T] {
  import QSUPattern._

  private def mergeSources(left: QSUGraph, right: QSUGraph)
      : SrcMerge[QSUGraph, FreeQS] =
    slamdata.Predef.???

  val graduateƒ: Coalgebra[QScriptEducated, QSUGraph] =
    Recursive[QSUGraph, QSUPattern[T, ?]].project(_) match {
      case QSUPattern(root, qsu) => qsu match {

        case QSU.Read(path) =>
          Inject[Const[Read[AFile], ?], QScriptEducated].inj(Const(Read(path)))

        case QSU.Map(source, fm) =>
          QCE(Map[T, QSUGraph](source, fm))

        case QSU.QSFilter(source, fm) =>
          QCE(Filter[T, QSUGraph](source, fm))

        case QSU.QSReduce(source, buckets, reducers, repair) =>
          QCE(Reduce[T, QSUGraph](source, buckets, reducers, repair))

        case QSU.LeftShift(source, struct, idStatus, repair) =>
          QCE(LeftShift[T, QSUGraph](source, struct, idStatus, repair))

        case QSU.UniformSort(source, buckets, order) =>
          QCE(Sort[T, QSUGraph](source, buckets, order))

        case QSU.Union(left, right) =>
          val SrcMerge(source, lBranch, rBranch) = mergeSources(left, right)
          QCE(Union[T, QSUGraph](source, lBranch, rBranch))

        case QSU.Subset(from, op, count) =>
          val SrcMerge(source, fromBranch, countBranch) = mergeSources(from, count)
          QCE(Subset[T, QSUGraph](source, fromBranch, op, countBranch))

        case QSU.ThetaJoin(left, right, condition, joinType) =>
          val SrcMerge(source, lBranch, rBranch) = mergeSources(left, right)

          val combine: JoinFunc =
             Free.roll(MFC(ConcatMaps(
               Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Left.name), LeftSideF))),
               Free.roll(MFC(MakeMap(StrLit[T, JoinSide](JoinDir.Right.name), RightSideF))))))

          Inject[ThetaJoin, QScriptEducated].inj(
            ThetaJoin[T, QSUGraph](source, lBranch, rBranch, condition, joinType, combine))

        case _ => scala.sys.error("found an LPish thing")
      }
    }

  def graduate(graph: QSUGraph): T[QScriptEducated] =
    Corecursive[T[QScriptEducated], QScriptEducated].ana[QSUGraph](graph)(graduateƒ)
}
