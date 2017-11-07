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

package quasar.qscript.qsu

import quasar.NameGenerator
import quasar.Planner.PlannerErrorME
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.{BirecursiveT, EqualT}
import scalaz.Monad
import scalaz.Scalaz._

final class LPtoQS[T[_[_]]: BirecursiveT: EqualT] extends QSUTTypes[T] {
  def apply[F[_]: Monad: PlannerErrorME: NameGenerator](lp: T[LogicalPlan])
      : F[T[QScriptEducated]] =
    for {
      read <- ReadLP[T, F].apply(lp)
      extracted <- ExtractFreeMap[T, F](read)
      authenticated <- ApplyProvenance[T].apply[F](extracted)
      reified <- ReifyProvenance[T].apply[F](authenticated)
      graduated <- Graduate[T].apply[F](reified.graph)
    } yield graduated
}

object LPtoQS {
  def apply[T[_[_]]: BirecursiveT: EqualT]: LPtoQS[T] = new LPtoQS[T]
}
