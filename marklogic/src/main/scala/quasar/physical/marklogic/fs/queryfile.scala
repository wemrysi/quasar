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
import quasar.physical.marklogic.xquery._
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
      f: MainModule => ContentSourceIO[A]
    ): Free[S, (PhaseResults, FileSystemError \/ A)] = {
      type PrologsT[F[_], A] = WriterT[F, Prologs, A]
      type M[A] = PrologsT[Free[S, ?], A]

      // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
      //import EitherT.eitherTMonad

      def phase(main: MainModule): PhaseResults =
        Vector(PhaseResult.Detail("XQuery", main.render))

      val listContents: ConvertPath.ListContents[Free[S, ?]] =
        adir => lift(ContentSourceIO.runSessionIO(ops.ls(adir))).into[S].liftM[FileSystemErrT]

      def plan(qs: Fix[QScriptTotal[Fix, ?]]): PlanningT[Free[S, ?], MainModule] = {
        val planRes =
          qs.cataM(Planner[QScriptTotal[Fix, ?], XQuery].plan[WriterT[Free[S, ?], Prologs, ?]])
            .run.run.run

        EitherT(WriterT(planRes map { case (prologs, (phases, res)) =>
          (phases, res map (MainModule(Version.`1.0-ml`, prologs, _)))
        }))
      }

      val planning = for {
        qs  <- convertToQScript(some(listContents))(lp)
        mod <- plan(qs).leftMap(planningFailed(lp, _))
        a   <- WriterT.put(lift(f(mod)).into[S])(phase(mod)).liftM[FileSystemErrT]
      } yield a

      planning.run.run
    }

    def exec(lp: Fix[LogicalPlan], out: AFile) =
      plannedLP(lp)(κ(out.point[ContentSourceIO]))

    def eval(lp: Fix[LogicalPlan]) =
      plannedLP(lp)(main =>
        ContentSourceIO.resultCursor(
          SessionIO.evaluateModule_(main),
          resultsChunkSize))

    def explain(lp: Fix[LogicalPlan]) =
      plannedLP(lp)(main => ExecutionPlan(FsType, main.render).point[ContentSourceIO])

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
