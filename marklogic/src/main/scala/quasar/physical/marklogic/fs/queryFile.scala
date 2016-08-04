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
import quasar.fs._
import quasar.effect.Read
import quasar.physical.marklogic._
import quasar.physical.marklogic.qscript._
import quasar.Planner.PlannerError
import quasar.qscript._

import scala.Predef.???

import matryoshka._, Recursive.ops._
import pathy.Path._
import scalaz._, Scalaz._, concurrent._

object queryfile {
  import QueryFile._

  def interpret[S[_]](
    implicit
    S0: Read[Client, ?] :<: S,
    S1: Task :<: S
  ): QueryFile ~> Free[S, ?] = new (QueryFile ~> Free[S, ?]) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) => (
          for {
            qs     <- EitherT(convertToQScript(lp).point[Free[S, ?]])
            _      =  println(s"queryfile interpret qs: $qs")
            xquery <- EitherT(qs.cataM(
                     Planner.Planner[QScriptTotal[Fix, ?], String].plan).point[Free[S, ?]])
            _    <- Client.execute(xquery, fileParent(out) </> dir(fileName(out).value)).liftM[EitherT[?[_], PlannerError, ?]]
          } yield out
        ).leftMap(FileSystemError.planningFailed(lp,_)).run.strengthL(Vector.empty)

      case EvaluatePlan(lp) =>
        ???

      case More(h) =>
        ???

      case Close(h) =>
        ???

      case Explain(lp) =>
        ???

      case ListContents(dir) =>
        ???

      case FileExists(file) =>
        ???
    }
  }
}
