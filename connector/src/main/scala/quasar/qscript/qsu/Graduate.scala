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

import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.qscript.{
  educatedToTotal,
  Filter,
  Hole,
  LeftShift,
  Map,
  QCE,
  Read,
  Reduce,
  Sort,
  SrcHole,
  SrcMerge,
  Subset,
  ThetaJoin,
  Union}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.QSUGraph.QSUPattern

import matryoshka.{Corecursive, CorecursiveT, Coalgebra, Recursive}
import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import scalaz.{~>, -\/, Const, Inject, NaturalTransformation}

final class Graduate[T[_[_]]: CorecursiveT] extends QSUTTypes[T] {
  import QSUPattern._

  type QSE[A] = QScriptEducated[A]
  private type QSU[A] = QScriptUniform[A]

  private def mergeSources(left: QSUGraph, right: QSUGraph): SrcMerge[QSUGraph, FreeQS] = {
    val root: Symbol = IntersectGraphs.intersect[QSU](left.vertices, right.vertices)

    SrcMerge(
      QSUGraph[T](root, left.vertices),
      graduateCoEnv(root, left),
      graduateCoEnv(root, right))
  }

  private def educate(qsu: QSU[QSUGraph]): QSE[QSUGraph] =
    qsu match {
      case QSU.Read(path) =>
        Inject[Const[Read[AFile], ?], QSE].inj(Const(Read(path)))

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

      case QSU.Distinct(source) => slamdata.Predef.??? // TODO

      case QSU.Nullary(mf) => slamdata.Predef.??? // TODO

      case QSU.ThetaJoin(left, right, condition, joinType, combiner) =>
        val SrcMerge(source, lBranch, rBranch) = mergeSources(left, right)

        Inject[ThetaJoin, QSE].inj(
          ThetaJoin[T, QSUGraph](source, lBranch, rBranch, condition, joinType, combiner))

      case qsu =>
        scala.sys.error(s"Found an unexpected LP-ish $qsu.") // TODO use Show to print
    }

  private def graduateƒ[F[_]](halt: Option[(Symbol, F[QSUGraph])])(lift: QSE ~> F)
      : Coalgebra[F, QSUGraph] = graph => {

    val pattern: QSUPattern[T, QSUGraph] =
      Recursive[QSUGraph, QSUPattern[T, ?]].project(graph)

    def default: F[QSUGraph] = lift(educate(pattern.qsu))

    halt match {
      case Some((name, output)) =>
        pattern match {
          case QSUPattern(`name`, _) => output
          case _ => default
        }
      case None => default
    }
  }

  private def graduateCoEnv(source: Symbol, graph: QSUGraph): FreeQS = {
    type CoEnvTotal[A] = CoEnv[Hole, QScriptTotal, A]

    val halt: Option[(Symbol, CoEnvTotal[QSUGraph])] =
      Some((source, CoEnv.coEnv(-\/(SrcHole))))

    val lift: QSE ~> CoEnvTotal =
      educatedToTotal[T].inject andThen PrismNT.coEnv[QScriptTotal, Hole].reverseGet

    Corecursive[FreeQS, CoEnvTotal].ana[QSUGraph](graph)(
      graduateƒ[CoEnvTotal](halt)(lift))
  }

  def apply(graph: QSUGraph): T[QSE] =
    Corecursive[T[QSE], QSE].ana[QSUGraph](graph)(
      graduateƒ[QSE](None)(NaturalTransformation.refl[QSE]))
}

object Graduate {
  def apply[T[_[_]]: CorecursiveT]: Graduate[T] =
    new Graduate[T]
}
