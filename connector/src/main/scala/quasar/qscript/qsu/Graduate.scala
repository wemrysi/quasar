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

import matryoshka.{Corecursive, CorecursiveT, CoalgebraM, Recursive}
import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import scalaz.{~>, -\/, Const, Inject, Monad, NaturalTransformation}
import scalaz.Scalaz._

final class Graduate[T[_[_]]: CorecursiveT] extends QSUTTypes[T] {
  import QSUPattern._

  type QSE[A] = QScriptEducated[A]
  private type QSU[A] = QScriptUniform[A]

  def apply[F[_]: Monad: PlannerErrorME](graph: QSUGraph): F[T[QSE]] =
    Corecursive[T[QSE], QSE].anaM[F, QSUGraph](graph)(
      graduateƒ[F, QSE](None)(NaturalTransformation.refl[QSE]))

  private def mergeSources[F[_]: Monad: PlannerErrorME](left: QSUGraph, right: QSUGraph)
      : SrcMerge[QSUGraph, F[FreeQS]] = {
    val root: Symbol = IntersectGraphs.intersect[QSU](left.vertices, right.vertices)

    SrcMerge(
      QSUGraph[T](root, left.vertices),
      graduateCoEnv[F](root, left),
      graduateCoEnv[F](root, right))
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  private def educate[F[_]: Monad: PlannerErrorME](qsu: QSU[QSUGraph])
      : F[QSE[QSUGraph]] =
    qsu match {
      case QSU.Read(path) =>
        Inject[Const[Read[AFile], ?], QSE].inj(Const(Read(path))).point[F]

      case QSU.Map(source, fm) =>
        QCE(Map[T, QSUGraph](source, fm)).point[F]

      case QSU.QSFilter(source, fm) =>
        QCE(Filter[T, QSUGraph](source, fm)).point[F]

      case QSU.QSReduce(source, buckets, reducers, repair) =>
        QCE(Reduce[T, QSUGraph](source, buckets, reducers, repair)).point[F]

      case QSU.LeftShift(source, struct, idStatus, repair) =>
        QCE(LeftShift[T, QSUGraph](source, struct, idStatus, repair)).point[F]

      case QSU.UniformSort(source, buckets, order) =>
        QCE(Sort[T, QSUGraph](source, buckets, order)).point[F]

      case QSU.Union(left, right) =>
        val SrcMerge(source, lBranch, rBranch) = mergeSources[F](left, right)
        (lBranch |@| rBranch)((l, r) =>
          QCE(Union[T, QSUGraph](source, l, r)))

      case QSU.Subset(from, op, count) =>
        val SrcMerge(source, fromBranch, countBranch) = mergeSources[F](from, count)
        (fromBranch |@| countBranch)((f, c) =>
          QCE(Subset[T, QSUGraph](source, f, op, c)))

      case QSU.Distinct(source) => slamdata.Predef.??? // TODO

      case QSU.Nullary(mf) => slamdata.Predef.??? // TODO

      case QSU.ThetaJoin(left, right, condition, joinType, combiner) =>
        val SrcMerge(source, lBranch, rBranch) = mergeSources[F](left, right)

        (lBranch |@| rBranch)((l, r) =>
          Inject[ThetaJoin, QSE].inj(
            ThetaJoin[T, QSUGraph](source, l, r, condition, joinType, combiner)))

      case qsu =>
        PlannerErrorME[F].raiseError(
          InternalError(s"Found an unexpected LP-ish $qsu.", None)) // TODO use Show to print
    }

  private def graduateƒ[F[_]: Monad: PlannerErrorME, G[_]](
    halt: Option[(Symbol, F[G[QSUGraph]])])(
    lift: QSE ~> G)
      : CoalgebraM[F, G, QSUGraph] = graph => {

    val pattern: QSUPattern[T, QSUGraph] =
      Recursive[QSUGraph, QSUPattern[T, ?]].project(graph)

    def default: F[G[QSUGraph]] = educate[F](pattern.qsu).map(lift)

    halt match {
      case Some((name, output)) =>
        pattern match {
          case QSUPattern(`name`, _) => output
          case _ => default
        }
      case None => default
    }
  }

  private def graduateCoEnv[F[_]: Monad: PlannerErrorME](source: Symbol, graph: QSUGraph)
      : F[FreeQS] = {
    type CoEnvTotal[A] = CoEnv[Hole, QScriptTotal, A]

    val halt: Option[(Symbol, F[CoEnvTotal[QSUGraph]])] =
      Some((source, CoEnv.coEnv[Hole, QScriptTotal, QSUGraph](-\/(SrcHole)).point[F]))

    val lift: QSE ~> CoEnvTotal =
      educatedToTotal[T].inject andThen PrismNT.coEnv[QScriptTotal, Hole].reverseGet

    Corecursive[FreeQS, CoEnvTotal].anaM[F, QSUGraph](graph)(
      graduateƒ[F, CoEnvTotal](halt)(lift))
  }
}

object Graduate {
  def apply[T[_[_]]: CorecursiveT]: Graduate[T] =
    new Graduate[T]
}
