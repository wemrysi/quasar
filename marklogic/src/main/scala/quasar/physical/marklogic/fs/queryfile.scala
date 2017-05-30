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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.{Planner => QPlanner, RenderTreeT}
import quasar.common._
import quasar.contrib.pathy._
import quasar.fp._
import quasar.fs._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.physical.marklogic.{qscript => mlqscript}, mlqscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript._

import com.marklogic.xcc.types.XSString
import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

object queryfile {
  import QueryFile._

  type MLQScriptCP[T[_[_]]] = (
    QScriptCore[T, ?]           :\:
    ThetaJoin[T, ?]             :\:
    Const[ShiftedRead[ADir], ?] :/:
    Const[Read[AFile], ?]
  )

  type MLQScript[T[_[_]], A] = MLQScriptCP[T]#M[A]

  implicit def mlQScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[MLQScript[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[Read[AFile], ?]]))

  def lpToQScript[
    F[_]   : Monad: MonadFsErr: PhaseResultTell: Xcc,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    FMT: SearchOptions
  ](lp: T[LogicalPlan]): F[T[MLQScript[T, ?]]] = {
    type MLQ[A]  = MLQScript[T, A]
    type QSR[A]  = QScriptRead[T, A]
    val O = new Optimize[T]
    val R = new Rewrite[T]

    for {
      qs        <- convertToQScriptRead[T, F, QSR](ops.directoryContents[F, FMT])(lp)
      shifted   <- Unirewrite[T, MLQScriptCP[T], F](R, ops.directoryContents[F, FMT]).apply(qs)
      _         <- logPhase(PhaseResult.tree("QScript (ShiftRead)", shifted))
      optimized =  shifted.transHylo(O.optimize(reflNT[MLQ]), Unicoalesce[T, MLQScriptCP[T]])
      _         <- logPhase(PhaseResult.tree("QScript (Optimized)", optimized))
    } yield optimized
  }

  def prettyPrint[F[_]: Monad: MonadFsErr: Xcc](xqy: XQuery): F[Option[XQuery]] = {
    val prettyPrinted =
      Xcc[F].queryResults(xdmp.prettyPrint(XQuery(s"'$xqy'")))
        .map(_.headOption collect { case s: XSString => XQuery(s.asString) })

    Xcc[F].handleWith(prettyPrinted) {
      case err @ XccError.QueryError(_, cause) =>
        MonadFsErr[F].raiseError(
          FileSystemError.qscriptPlanningFailed(QPlanner.InternalError(
            err.shows, Some(cause))))

      // NB: As this is only for pretty printing, if we fail for some other reason
      //     just return an empty result.
      case _ => none[XQuery].point[F]
    }
  }

  ////

  private def logPhase[F[_]](pr: PhaseResult)(implicit
    PRT: PhaseResultTell[F]
  ): F[Unit] = PRT.tell(Vector(pr))

  private def saveTo[F[_]: Bind: PrologW, T](
    dst: AFile, results: XQuery
  )(implicit SP: StructuralPlanner[F, T]): F[XQuery] =
    SP.seqToArray(results) map (xdmp.documentInsert(pathUri(dst).xs, _))
}
