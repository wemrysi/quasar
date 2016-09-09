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
import quasar.fp._
import quasar.fp.free.lift
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl.queryFileFromDataCursor
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

  // TODO: Still need to implement ExecutePlan.
  def interpret[S[_]](
    resultsChunkSize: Positive
  )(implicit
    S0: SessionIO :<: S,
    S1: ContentSourceIO :<: S,
    S2: Task :<: S,
    S3: MLResultHandles :<: S,
    S4: MonotonicSeq :<: S
  ): QueryFile ~> Free[S, ?] = {
    def plannedLP[A](
      lp: Fix[LogicalPlan])(
      f: XQuery => ContentSourceIO[A]
    ): Free[S, (PhaseResults, FileSystemError \/ A)] = {
      // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
      import EitherT.eitherTMonad

      def phase(xqy: XQuery): PhaseResults =
        Vector(PhaseResult.Detail("XQuery", xqy.toString))

      val listContents: DiscoverPath.ListContents[Free[S, ?]] =
        adir => lift(ContentSourceIO.runSessionIO(ops.ls(adir))).into[S].liftM[FileSystemErrT]

      val planning = for {
        qs  <- convertToQScriptRead[Fix, Free[S, ?], QScriptInternalRead[Fix, ?]](listContents)(lp)
        xqy <- qs.cataM(Planner[QScriptInternalRead[Fix, ?], XQuery].plan[Free[S, ?]])
                 .leftMap(planningFailed(lp, _))
        a   <- WriterT.put(lift(f(xqy)).into[S])(phase(xqy)).liftM[FileSystemErrT]
      } yield a

      planning.run.run
    }

    def exec(lp: Fix[LogicalPlan], out: AFile) =
      plannedLP(lp)(κ(out.point[ContentSourceIO]))

    def eval(lp: Fix[LogicalPlan]) =
      plannedLP(lp)(xqy =>
        ContentSourceIO.resultCursor(
          SessionIO.evaluateQuery_(xqy),
          resultsChunkSize))

    def explain(lp: Fix[LogicalPlan]) =
      plannedLP(lp)(xqy =>
        ExecutionPlan(FsType, xqy.toString).point[ContentSourceIO])

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
