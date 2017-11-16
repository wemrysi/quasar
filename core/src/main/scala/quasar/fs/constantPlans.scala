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

package quasar.fs

import slamdata.Predef._
import quasar.{Data, refineConstantPlan}
import quasar.common.PhaseResult
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs.QueryFile._

import scalaz._, Scalaz._

object constantPlans {

  type State[A] = KeyValueStore[QueryFile.ResultHandle, Vector[Data], A]

  val constantPhase =
    PhaseResult.detail("Intercept Constant", "This plan is constant and can be evaluated in memory")

  def queryFile[S[_]](
    implicit
    S0: QueryFile :<: S,
    S1: ManageFile :<: S,
    seq: MonotonicSeq.Ops[S],
    write: WriteFile.Ops[S],
    state: KeyValueStore.Ops[QueryFile.ResultHandle, Vector[Data], S]
  ): QueryFile ~> Free[S, ?] = {

    val query = QueryFile.Ops[S]
    val queryUnsafe = QueryFile.Unsafe[S]

    def dataHandle(data: List[Data]): Free[S, ResultHandle] =
      for {
        h <- seq.next.map(ResultHandle(_))
        _ <- state.put(h, data.toVector)
      } yield h

    λ[QueryFile ~> Free[S, ?]] {
      case ExecutePlan(lp, out) =>
        refineConstantPlan(lp).fold(
          // I believe it is safe to call void here because we generated the data
          data => write.saveThese(out, data.toVector).void.run.strengthL(Vector(constantPhase)),
          lp   => query.execute(lp, out).run.run)

      case EvaluatePlan(lp) =>
        refineConstantPlan(lp).fold(
          data => dataHandle(data).map(h => (Vector(constantPhase), h.right)),
          lp   => queryUnsafe.eval(lp).run.run)

      case More(handle) =>
        state.get(handle).run.flatMap {
          case Some(data) => state.put(handle, Vector.empty).as(data.right)
          case None       => queryUnsafe.more(handle).run
        }

      case Close(handle) =>
        state.contains(handle).ifM(state.delete(handle), queryUnsafe.close(handle))

      case Explain(lp) =>
        val constantExecutionPlan =
          ExecutionPlan(FileSystemType("constant"), "none", ISet.empty)
        refineConstantPlan(lp).fold(
          data => (Vector(constantPhase), constantExecutionPlan.right[FileSystemError]).point[Free[S, ?]],
          lp   => query.explain(lp).run.run)

      case ListContents(dir) =>
        query.listContents(dir).run

      case FileExists(file) =>
        query.fileExists(file)
    }
  }
}