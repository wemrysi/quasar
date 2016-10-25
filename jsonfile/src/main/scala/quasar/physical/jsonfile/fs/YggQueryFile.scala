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
import quasar.{ qscript => q }
import matryoshka._, Recursive.ops._
import QueryFile._
import LogicalPlan._
import quasar.qscript._
// import quasar.contrib.matryoshka.ShowT

class YggQueryFile[S[_]](implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) extends STypesFree[S, Fix] {
  private def phaseResults(msg: String): F[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", msg))
  private def TODO[A]: FLR[A]                            = Unimplemented

  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]): QueryFile ~> FS = {
    def lpResultƒ: AlgebraM[FLR, LogicalPlan, QRep] = {
      case ReadF(path)                           => TODO
      case ConstantF(data)                       => TODO
      case InvokeF(func, values)                 => TODO
      case FreeF(sym)                            => TODO
      case LetF(ident, form, in)                 => TODO
      case TypecheckF(expr, typ, cont, fallback) => TODO
    }

    def executePlan(lp: FixPlan, out: AFile): FLR[AFile] = TODO
    def evaluatePlan(lp: FixPlan): FLR[QHandle]          = TODO
    def explainPlan(lp: FixPlan): FLR[ExecutionPlan]     = ExecutionPlan(FsType, lp.to_s)

    λ[QueryFile ~> FS] {
      case Explain(lp)          => phaseResults(lp.to_s) tuple explainPlan(lp)
      case EvaluatePlan(lp)     => phaseResults(lp.to_s) tuple evaluatePlan(lp)
      case ExecutePlan(lp, out) => phaseResults(lp.to_s) tuple executePlan(lp, out)
      case ListContents(dir)    => ls(dir)
      case FileExists(file)     => KVF contains file
      case More(qh)             => Vector()
      case Close(fh)            => (KVQ delete fh).void
    }
  }
}

class QScriptCorePlanner[F[_]: Applicative, T[_[_]]] extends Planner[F, QScriptCore[T, ?]] {
  private def TODO: F[QRep] = ???

  def plan: AlgebraM[F, QScriptCore[T, ?], Table] = {
    case q.Map(src, f)                           => TODO
    case q.LeftShift(src, struct, repair)        => TODO
    case q.Reduce(src, bucket, reducers, repair) => TODO
    case q.Sort(src, bucket, order)              => TODO
    case q.Filter(src, f)                        => TODO
    case q.Union(src, lBranch, rBranch)          => TODO
    case q.Subset(src, from, q.Drop, count)      => TODO
    case q.Subset(src, from, q.Take, count)      => TODO
    case q.Subset(src, from, q.Sample, count)    => TODO
    case q.Unreferenced()                        => TODO
  }
}
