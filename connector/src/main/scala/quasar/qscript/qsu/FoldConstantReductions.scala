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

import slamdata.Predef._
import ApplyProvenance.AuthenticatedQSU
import quasar._
// import quasar.contrib.matryoshka._
import quasar.ejson.EJson
import quasar.fp._
import quasar.qscript._
import quasar.Planner._
import ReduceFuncs.{Arbitrary, First, Last}

import matryoshka.{Hole => _, _}
import matryoshka.data.free._
import matryoshka.implicits._
import scalaz._, Scalaz._

final class FoldConstantReductions[T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors
  private val func = qscript.construction.Func[T]
  private val json = ejson.Fixed[T[EJson]]

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def apply[F[_]: Monad: PlannerErrorME](aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] = {
    type G[A] = StateT[StateT[F, RevIdx, ?], QAuth, A]
    aqsu.graph.rewriteM[G](Function.unlift(extract[G])).run(aqsu.auth).eval(aqsu.graph.generateRevIndex) map {
      case (auth, graph) => ApplyProvenance.AuthenticatedQSU(graph, auth)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private def extract[F[_]: Monad: PlannerErrorME: RevIdxM]
      : QSUGraph => Option[F[QSUGraph]] = {
    case qsr@Extractors.LPReduce(m@Extractors.Map(_, fm), Arbitrary(_) | First(_) | Last(_)) =>
      val normalizedFM = fm.transCata[FreeMap](MapFuncCore.normalize[T, Hole])
      normalizedFM.project.run match {
        case \/-(MFC(MapFuncsCore.Constant(_))) =>
          Some(qsr.overwriteAtRoot(m.unfold.map(_.root)).pure[F])
        case _ => None
      }
    case _ => None
  }

}

object FoldConstantReductions {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: RenderTreeT: ShowT,
      F[_]: Monad: NameGenerator: PlannerErrorME]
      (aqsu: AuthenticatedQSU[T])
      : F[AuthenticatedQSU[T]] =
    taggedInternalError("FoldConstantReductions", new FoldConstantReductions[T].apply[F](aqsu))
}