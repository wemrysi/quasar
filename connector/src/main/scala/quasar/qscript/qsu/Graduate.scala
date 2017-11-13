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
import quasar.contrib.pathy.AFile
import quasar.fp._
import quasar.qscript.{
  educatedToTotal,
  Filter,
  Hole,
  HoleF,
  LeftShift,
  Map,
  QCE,
  Read,
  Reduce,
  ReduceFuncs,
  ReduceIndexF,
  Sort,
  SrcHole,
  SrcMerge,
  Subset,
  ThetaJoin,
  Union,
  Unreferenced}
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.QSUGraph.QSUPattern

import matryoshka.{Corecursive, CorecursiveT, CoalgebraM, Recursive}
import matryoshka.data.free._
import matryoshka.patterns.CoEnv
import scalaz.{~>, -\/, \/-, Const, Inject, Monad, NaturalTransformation}
import scalaz.Scalaz._

final class Graduate[T[_[_]]: CorecursiveT] extends QSUTTypes[T] {
  import QSUPattern._

  type QSE[A] = QScriptEducated[A]
  private type QSU[A] = QScriptUniform[A]

  def apply[F[_]: Monad: PlannerErrorME: NameGenerator](graph: QSUGraph): F[T[QSE]] =
    Corecursive[T[QSE], QSE].anaM[F, QSUGraph](graph)(
      graduateƒ[F, QSE](None)(NaturalTransformation.refl[QSE]))

  private def mergeSources[F[_]: Monad: PlannerErrorME: NameGenerator](
      left: QSUGraph,
      right: QSUGraph): F[SrcMerge[QSUGraph, FreeQS]] = {

    val lvert = left.vertices
    val rvert = right.vertices

    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def lub(
        lefts: Set[Symbol],
        rights: Set[Symbol],
        visited: Set[Symbol]): Set[Symbol] = {

      val lnodes = lefts.map(lvert)
      val rnodes = rights.map(rvert)

      val lexp = lnodes.flatMap(_.foldLeft(Set[Symbol]())(_ + _))
      val rexp = rnodes.flatMap(_.foldLeft(Set[Symbol]())(_ + _))

      val check: Set[Symbol] =
        (lexp intersect visited) union
        (rexp intersect visited) union
        (lexp intersect rexp)

      if (!check.isEmpty)
        check
      else
        lub(lexp, rexp, visited.union(lexp).union(rexp))
    }

    val source: Set[Symbol] = if (left.root === right.root)
      Set(left.root)
    else
      lub(Set(left.root), Set(right.root), Set(left.root, right.root))

    // we merge the vertices in the result, just in case the graphs are
    // additively applied to a common root
    val mergedVertices: QSUVerts[T] = left.vertices ++ right.vertices

    source.headOption match {
      case hole @ Some(root) =>
        for {
          lGrad <- graduateCoEnv[F](hole, left)
          rGrad <- graduateCoEnv[F](hole, right)
        } yield SrcMerge(QSUGraph[T](root, mergedVertices), lGrad, rGrad)

      case None =>
        for {
          lGrad <- graduateCoEnv[F](None, left)
          rGrad <- graduateCoEnv[F](None, right)
          name <- NameGenerator[F].prefixedName("merged")
        } yield {
          val root: Symbol = Symbol(name)

          val newVertices: QSUVerts[T] =
            mergedVertices + (root -> QSU.Unreferenced[T, Symbol]())

          SrcMerge(QSUGraph[T](root, newVertices), lGrad, rGrad)
        }
    }
  }

  private def educate[F[_]: Monad: PlannerErrorME: NameGenerator](qsu: QSU[QSUGraph])
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

      case QSU.QSSort(source, buckets, order) =>
        QCE(Sort[T, QSUGraph](source, buckets, order)).point[F]

      case QSU.Union(left, right) =>
        mergeSources[F](left, right) map {
          case SrcMerge(source, lBranch, rBranch) =>
            QCE(Union[T, QSUGraph](source, lBranch, rBranch))
        }

      case QSU.Subset(from, op, count) =>
        mergeSources[F](from, count) map {
          case SrcMerge(source, fromBranch, countBranch) =>
            QCE(Subset[T, QSUGraph](source, fromBranch, op, countBranch))
        }

      // TODO distinct should be its own node in qscript proper
      case QSU.Distinct(source) =>
        QCE(Reduce[T, QSUGraph](
          source,
          List(HoleF),
          List(ReduceFuncs.Arbitrary(HoleF)),
          ReduceIndexF(\/-(0)))).point[F]

      case QSU.Unreferenced() =>
        QCE(Unreferenced[T, QSUGraph]()).point[F]

      case QSU.ThetaJoin(left, right, condition, joinType, combiner) =>
        mergeSources[F](left, right) map {
          case SrcMerge(source, lBranch, rBranch) =>
            Inject[ThetaJoin, QSE].inj(
              ThetaJoin[T, QSUGraph](source, lBranch, rBranch, condition, joinType, combiner))
        }

      case qsu =>
        PlannerErrorME[F].raiseError(
          InternalError(s"Found an unexpected LP-ish $qsu.", None)) // TODO use Show to print
    }

  private def graduateƒ[F[_]: Monad: PlannerErrorME: NameGenerator, G[_]](
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

  private def graduateCoEnv[F[_]: Monad: PlannerErrorME: NameGenerator]
    (hole: Option[Symbol], graph: QSUGraph)
      : F[FreeQS] = {
    type CoEnvTotal[A] = CoEnv[Hole, QScriptTotal, A]

    val halt: Option[(Symbol, F[CoEnvTotal[QSUGraph]])] =
      hole.map((_, CoEnv.coEnv[Hole, QScriptTotal, QSUGraph](-\/(SrcHole)).point[F]))

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
