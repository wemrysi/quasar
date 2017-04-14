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

package quasar.physical.sparkcore.fs

import quasar.fs._
import quasar.frontend.logicalplan.LogicalPlan
import quasar.qscript.analysis._

import matryoshka.{Hole => _, _}
import matryoshka.data.Fix
import scalaz._

object analyze {

  def interpreter[S[_], F[_] : Traverse, T](toQS: Fix[LogicalPlan] => T)(implicit
    R: Recursive.Aux[T, F],
    CA: Cardinality[F],
    CO: Cost[F],
    Q: QueryFile.Ops[S]
  ): Analyze ~> Free[S, ?] = new (Analyze ~> Free[S, ?]) {

    def apply[A](from: Analyze[A]) = from match {
      case Analyze.QueryCost(lp) =>
        R.zygoM(toQS(lp))(CA.calculate(pathCard[S]), CO.evaluate(pathCard[S])).run
    }
  }


}
