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
import quasar.common.JoinType
import quasar.fp.ski.κ
import quasar.qscript.{HoleF, IncludeId, LeftSideF, MFC, ReduceIndexF, RightSideF}
import quasar.qscript.ReduceFunc._
import quasar.qscript.MapFuncsCore.ConcatArrays
import quasar.qscript.qsu.{QScriptUniform => QSU}
import quasar.qscript.qsu.ApplyProvenance.AuthenticatedQSU

import matryoshka.{BirecursiveT, EqualT}
import scalaz.{\/-, Applicative, Free, ICons, INil, Monad}
import scalaz.Scalaz._

final class ReifyProvenance[T[_[_]]: BirecursiveT: EqualT] extends QSUTTypes[T] {

  type QSU[A] = QScriptUniform[A]

  val prov = new QProv[T]

  private def toQScript[F[_]: Applicative: PlannerErrorME](dims: QSUDims[T])
      : QSU[Symbol] => F[QSU[Symbol]] = {
    case QSU.AutoJoin(sources, combiner0) =>
      sources.list match {
        case ICons(left, ICons(right, INil())) =>
          val condition: JoinFunc =
            prov.autojoinCondition(dims(left), dims(right))(κ(HoleF))

          val combiner: JoinFunc = Free.roll(combiner0.map {
            case 0 => LeftSideF
            case 1 => RightSideF
            case n => scala.sys.error(s"AutoJoin combiner has wrong size $n. Expected size 2.")
          })

          val qsu: QSU[Symbol] = QSU.ThetaJoin(left, right, condition, JoinType.Inner, combiner)

          qsu.point[F]

        // TODO support autojoins of any size - requires name generation
        case _ =>
          PlannerErrorME[F].raiseError(
            InternalError(s"AutoJoin on a list of size ${sources.size} is not supported.", None))
      }

    case QSU.Transpose(source, _) =>
      // TODO only reify the identity/value information when it's used
      val repair: JoinFunc = Free.roll(MFC(ConcatArrays(LeftSideF, RightSideF)))
      val qsu: QSU[Symbol] = QSU.LeftShift[T, Symbol](source, HoleF, IncludeId, repair)

      qsu.point[F]

    case QSU.LPReduce(source, reduce) =>
      val bucket: FreeMap = slamdata.Predef.??? // TODO computed from provenance
      val qsu: QSU[Symbol] = QSU.QSReduce[T, Symbol](source, List(bucket), List(reduce.map(κ(HoleF))), ReduceIndexF(\/-(0)))

      qsu.point[F]

    case qsu => qsu.point[F]
  }

  def apply[F[_]: Monad: PlannerErrorME](qsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    for {
      vertices <- qsu.graph.vertices.traverse[F, QSU[Symbol]](toQScript[F](qsu.dims))
    } yield {
      AuthenticatedQSU[T](QSUGraph(qsu.graph.root, vertices), qsu.dims)
    }
}

object ReifyProvenance {
  def apply[T[_[_]]: BirecursiveT: EqualT]: ReifyProvenance[T] =
    new ReifyProvenance[T]
}
