/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.LogicalPlan
import quasar.PhaseResult
import quasar.Planner.PlannerError
import quasar.effect.MonotonicSeq
import quasar.fs._
import quasar.fs.impl.queryFileFromDataCursor
import quasar.fp.numeric.Positive
import quasar.physical.marklogic._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc.{ChunkedResultSequence, SessionR, XccFailure}
import quasar.qscript._

import com.marklogic.xcc.RequestOptions
import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._, concurrent._

object queryfile {
  import QueryFile._
  import FileSystemError._
  import MarkLogicPlanner._

  def interpret[S[_]](
    resultsChunkSize: Positive
  )(implicit
    S0: SessionR :<: S,
    S1: XccFailure :<: S,
    S2: MLResultHandles :<: S,
    S3: MonotonicSeq :<: S,
    S4: Task :<: S,
    S5: XccCursorM :<: S
  ): QueryFile ~> Free[S, ?] = {
    val session = xcc.session.Ops[S]

    val evalOpts = {
      val ropts = new RequestOptions
      ropts.setCacheResult(false)
      ropts
    }

    def planLP(lp: Fix[LogicalPlan]): PlannerError \/ XQuery =
      convertToQScript(lp) >>= (_.cataM(Planner[QScriptTotal[Fix, ?], XQuery].plan))

    def exec(lp: Fix[LogicalPlan], out: AFile) =
      planLP(lp).fold(
        err => (Vector.empty[PhaseResult], \/.left(planningFailed(lp, err))),
        xqy => (Vector(PhaseResult.Detail("XQuery", xqy)), \/.right(out))
      ).point[Free[S, ?]]

    // TODO: PhaseResults
    def eval(lp: Fix[LogicalPlan]) =
      EitherT.fromDisjunction[Free[S, ?]](planLP(lp))
        .leftMap(planningFailed(lp, _))
        .flatMap(session.evaluateQuery(_, evalOpts).liftM[FileSystemErrT])
        .map(new ChunkedResultSequence[XccCursor](resultsChunkSize, _))
        .run
        .strengthL(Vector.empty[PhaseResult])

    // TODO: Eliminate duplication with exec
    def explain(lp: Fix[LogicalPlan]) =
      planLP(lp).fold(
        err => (Vector.empty[PhaseResult], \/.left(planningFailed(lp, err))),
        xqy => (Vector(PhaseResult.Detail("XQuery", xqy)), \/.right(ExecutionPlan(FsType, xqy)))
      ).point[Free[S, ?]]

    queryFileFromDataCursor[S, XccCursorM, ChunkedResultSequence[XccCursor]](exec, eval, explain,
      dir  => Set[PathSegment]().point[FileSystemErrT[Free[S, ?], ?]].run,
      file => false.point[Free[S, ?]])
  }
}
