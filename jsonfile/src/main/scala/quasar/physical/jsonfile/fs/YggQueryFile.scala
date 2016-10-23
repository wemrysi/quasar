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

package quasar.physical.jsonfile.fs

import quasar._
import quasar.Predef._
import quasar.fs._
import quasar.effect._
import scalaz._, Scalaz._
import FileSystemIndependentTypes._

trait YggQueryFile[S[_]] {
  self: FsAlgebras[S] =>

  import QueryFile._

  private def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))

  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) = λ[QueryFile ~> FS] {
    case ExecutePlan(lp, out) => phaseResults(lp) tuple \/-(out)
    case EvaluatePlan(lp)     => (phaseResults(lp) |@| nextLong)((ph, uid) => ph -> \/-(QHandle(uid)))
    case Explain(lp)          => phaseResults(lp) tuple ExecutionPlan(FsType, "...")
    case ListContents(dir)    => ls(dir)
    case FileExists(file)     => KVF contains file
    case More(qh)             => Vector()
    case Close(fh)            => (KVQ delete fh).void
  }
}
