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
import matryoshka._, Recursive.ops._
import QueryFile._
import LogicalPlan._
// import pathy._, Path._, quasar.contrib.pathy._

class YggQueryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) extends STypes[S] {
  private def phaseResults(msg: String): F[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", msg))
  private def phaseTodo[A](x: FLR[A]): FPLR[A]           = phaseResults("<jsonfile>") tuple x
  private def TODO[A] : FLR[A]                           = Unimplemented

  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]): QueryFile ~> FS = {
    def lpResultƒ: AlgebraM[FLR, LogicalPlan, Rep] = {
      case ReadF(path)                           => TODO
      case ConstantF(data)                       => TODO
      case InvokeF(func, values)                 => TODO
      case FreeF(sym)                            => TODO
      case LetF(ident, form, in)                 => TODO
      case TypecheckF(expr, typ, cont, fallback) => TODO
    }

    def evaluatePlan(lp: Fix[LogicalPlan]): FLR[QHandle] = TODO

    /** See marklogic's QScriptCorePlanner.scala for a very clean example */
    def qscriptCore: AlgebraM[FPLR, QScriptCore, Rep] = {
      case q.Map(src, f)                           => phaseTodo(TODO)
      case q.LeftShift(src, struct, repair)        => phaseTodo(TODO)
      case q.Reduce(src, bucket, reducers, repair) => phaseTodo(TODO)
      case q.Sort(src, bucket, order)              => phaseTodo(TODO)
      case q.Filter(src, f)                        => phaseTodo(TODO)
      case q.Union(src, lBranch, rBranch)          => phaseTodo(TODO)
      case q.Subset(src, from, q.Drop, count)      => phaseTodo(TODO)
      case q.Subset(src, from, q.Take, count)      => phaseTodo(TODO)
      case q.Subset(src, from, q.Sample, count)    => phaseTodo(TODO)
      case q.Unreferenced()                        => phaseTodo(TODO)
    }

    λ[QueryFile ~> FS] {
      case Explain(lp)          => phaseTodo(TODO)
      case ExecutePlan(lp, out) => phaseTodo(TODO)
      case EvaluatePlan(lp)     => phaseResults("<jsonfile>") tuple evaluatePlan(lp)
      case ListContents(dir)    => ls(dir)
      case FileExists(file)     => KVF contains file
      case More(qh)             => Vector()
      case Close(fh)            => (KVQ delete fh).void
    }
  }
}
