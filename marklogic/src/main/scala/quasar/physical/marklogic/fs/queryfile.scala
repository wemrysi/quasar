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
import quasar.SKI.κ
import quasar.{LogicalPlan, PhaseResult, PhaseResults}
import quasar.effect.MonotonicSeq
import quasar.fs._
import quasar.fs.impl.queryFileFromDataCursor
import quasar.fp.free.lift
import quasar.fp.numeric.Positive
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery.XQuery
import quasar.qscript._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._, concurrent._

object queryfile {
  import QueryFile._
  import FileSystemError._, PathError._
  import MarkLogicPlanner._

  def interpret[S[_]](
    resultsChunkSize: Positive
  )(implicit
    S0: SessionIO :<: S,
    S1: ContentSourceIO :<: S,
    S2: Task :<: S,
    S3: MLResultHandles :<: S,
    S4: MonotonicSeq :<: S
  ): QueryFile ~> Free[S, ?] = {
    def xqyPhase(xqy: XQuery): PhaseResults =
      Vector(PhaseResult.Detail("XQuery", xqy.toString))

    def plannedLP[F[_]: Applicative, A](
      lp: Fix[LogicalPlan])(
      f: XQuery => F[A]
    ): F[(PhaseResults, FileSystemError \/ A)] =
      convertToQScript(lp).flatMap(_.cataM(Planner[QScriptTotal[Fix, ?], XQuery].plan)).fold(
        err => (Vector.empty[PhaseResult], planningFailed(lp, err).left[A]).point[F],
        xqy => f(xqy) map (_.right[FileSystemError]) strengthL xqyPhase(xqy))

    def exec(lp: Fix[LogicalPlan], out: AFile) =
      plannedLP(lp)(κ(out.point[Free[S, ?]]))

    def eval(lp: Fix[LogicalPlan]) =
      lift(plannedLP(lp)(xqy => ContentSourceIO.resultCursor(
        SessionIO.evaluateQuery_(xqy),
        resultsChunkSize
      ))).into[S]

    def explain(lp: Fix[LogicalPlan]) =
      plannedLP(lp)(xqy => ExecutionPlan(FsType, xqy.toString).point[Free[S, ?]])

    def exists(file: AFile): Free[S, Boolean] =
      lift(ops.exists(file)).into[S]

    def listContents(dir: ADir): Free[S, FileSystemError \/ Set[PathSegment]] =
      lift(ops.exists(dir).ifM(
        ops.ls(dir).map(_.right[FileSystemError]),
        pathErr(pathNotFound(dir)).left[Set[PathSegment]].point[SessionIO]
      )).into[S]

    queryFileFromDataCursor[S, Task, ResultCursor](exec, eval, explain, listContents, exists)
  }
}
