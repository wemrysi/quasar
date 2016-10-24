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
import quasar.fs._, FileSystemError._
import quasar.effect._
import scalaz._, Scalaz._
import quasar.{ qscript => q }
import Planner.Rep
// import ygg.table._
import matryoshka._, Recursive.ops._
// import pathy.Path._
// import quasar.contrib.pathy._

class YggQueryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) extends STypes[S] {

  import QueryFile._

  private def phaseResults(msg: String): PhaseResults = Vector(PhaseResult.Detail("jsonfile", msg))
  private def pr(): PhaseResults                      = phaseResults("<jsonfile>")
  private def TODO[A] : FLR[A]                        = Unimplemented
  private def TODO_P[A] : FPLR[A]                     = pr().point[F] tuple TODO[A]

  def queryFile = λ[QueryFile ~> FS] {
    case Explain(lp)          => TODO_P
    case ExecutePlan(lp, out) => TODO_P
    case EvaluatePlan(lp)     => evaluatePlan(lp) map ((x: LR[QHandle]) => pr() -> x)
    case ListContents(dir)    => ls(dir)
    case FileExists(file)     => KVF contains file
    case More(qh)             => Vector()
    case Close(fh)            => (KVQ delete fh).void
  }

  def evaluatePlan(lp: Fix[LogicalPlan]): FLR[QHandle] = {
    import LogicalPlan._

    def lpResultƒ: AlgebraM[FLR, LogicalPlan, Rep] = {
      case ReadF(path)                           => TODO
      case ConstantF(data)                       => TODO
      case InvokeF(func, values)                 => TODO
      case FreeF(sym)                            => TODO
      case LetF(ident, form, in)                 => TODO
      case TypecheckF(expr, typ, cont, fallback) => TODO
    }

    TODO
  }

  /** See marklogic's QScriptCorePlanner.scala for a very clean example */
  def qscriptCore: AlgebraM[FPLR, QScriptCore, Rep] = {
    case q.Map(src, f)                           => TODO_P
    case q.LeftShift(src, struct, repair)        => TODO_P
    case q.Reduce(src, bucket, reducers, repair) => TODO_P
    case q.Sort(src, bucket, order)              => TODO_P
    case q.Filter(src, f)                        => TODO_P
    case q.Union(src, lBranch, rBranch)          => TODO_P
    case q.Subset(src, from, q.Drop, count)      => TODO_P
    case q.Subset(src, from, q.Take, count)      => TODO_P
    case q.Subset(src, from, q.Sample, count)    => TODO_P
    case q.Unreferenced()                        => TODO_P
  }
}
