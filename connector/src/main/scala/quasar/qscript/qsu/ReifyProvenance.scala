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

import slamdata.Predef.{Map => SMap, _}

import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.common.JoinType
import quasar.qscript.{
  construction,
  Center,
  HoleF,
  JoinSide,
  LeftSide3,
  LeftSideF,
  ReduceIndexF,
  RightSide3,
  RightSideF}
import quasar.qscript.provenance.Dimensions
import quasar.qscript.ReduceFunc._
import quasar.qscript.MapFuncsCore.StrLit
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka._
import scalaz.{\/-, Free, Monad, WriterT}
import scalaz.Scalaz._

final class ReifyProvenance[T[_[_]]: BirecursiveT: EqualT] extends QSUTTypes[T] {

  type QSU[A] = QScriptUniform[A]

  val prov = new QProv[T]
  val qsu  = QScriptUniform.Optics[T]
  val func = construction.Func[T]

  case class NewVertex(name: Symbol, value: QSU[Symbol], dims: Dimensions[prov.P])

  type X[F[_], A] = WriterT[F, List[NewVertex], A]

  private def toQScript[F[_]: Monad: PlannerErrorME: NameGenerator](dims: QSUDims[T])
      : QSU[Symbol] => X[F, QSU[Symbol]] = {

    case QSU.AutoJoin2(left, right, _combiner) =>
      val condition: FreeAccess[JoinSide] =
        prov.autojoinCondition(dims(left), dims(right))

      val combiner: JoinFunc =
        Free.roll(_combiner.map(Free.point(_)))

      qsu.thetaJoin(left, right, condition, JoinType.Inner, combiner).point[X[F, ?]]

    case QSU.AutoJoin3(left, center, right, combiner3) => WriterT.writerT {
      (NameGenerator[F].prefixedName("autojoin") |@|
        NameGenerator[F].prefixedName("leftAccess") |@|
        NameGenerator[F].prefixedName("centerAccess")) {

        case (joinName, lName, cName) =>

          val _condition: FreeAccess[JoinSide] =
            prov.autojoinCondition(dims(left), dims(center))

          def _combiner: JoinFunc =
            func.ConcatMaps(
              func.MakeMap(StrLit(lName), LeftSideF),
              func.MakeMap(StrLit(cName), RightSideF))

          def _join: QSU[Symbol] =
            qsu.thetaJoin(left, center, _condition, JoinType.Inner, _combiner)

          def projLeft[A](hole: FreeMapA[A]): FreeMapA[A] =
            func.ProjectKey(StrLit(lName), hole)

          def projCenter[A](hole: FreeMapA[A]): FreeMapA[A] =
            func.ProjectKey(StrLit(cName), hole)

          val combiner: JoinFunc = Free.roll(combiner3 map {
            case LeftSide3 => projLeft[JoinSide](LeftSideF)
            case Center => projCenter[JoinSide](LeftSideF)
            case RightSide3 => RightSideF
          })

          val newDims: Dimensions[prov.P] = prov.join(dims(left), dims(center))

          val condition: FreeAccess[JoinSide] =
            prov.autojoinCondition(newDims, dims(right))

          val sym = Symbol(joinName)
          val tjoin = qsu.thetaJoin(sym, right, condition, JoinType.Inner, combiner)
          val newVertex = NewVertex(sym, _join, newDims)

          (List(newVertex), tjoin)
      }
    }

    case QSU.LPReduce(source, reduce) =>
      val buckets = prov.buckets(prov.reduce(dims(source)))
      qsu.qsReduce(source, buckets, List(reduce as HoleF[T]), ReduceIndexF[T](\/-(0))).point[X[F, ?]]

    case QSU.QSSort(source, Nil, keys) =>
      val buckets = prov.buckets(prov.reduce(dims(source)))
      qsu.qsSort(source, buckets, keys).point[X[F, ?]]

    case other =>
      other.point[X[F, ?]]
  }

  def apply[F[_]: Monad: PlannerErrorME: NameGenerator](qsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {

    val pair: X[F, QSUVerts[T]] =
      qsu.graph.vertices.traverse(toQScript[F](qsu.dims))

    pair.run.map {
      case (nw, oldVertices) =>
        val (newV, newD) =
          nw.foldLeft[(QSUVerts[T], QSUDims[T])](SMap() -> SMap()) {
            case ((vAcc, dAcc), NewVertex(name, value, dims)) =>
              (vAcc + (name -> value), dAcc + (name -> dims))
          }
        AuthenticatedQSU[T](QSUGraph(qsu.graph.root, oldVertices ++ newV), qsu.dims ++ newD)
    }
  }
}

object ReifyProvenance {
  def apply[T[_[_]]: BirecursiveT: EqualT]: ReifyProvenance[T] =
    new ReifyProvenance[T]
}
