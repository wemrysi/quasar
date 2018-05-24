/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.mimir.evaluate

import quasar.{Data, RenderTreeT}
import quasar.api.ResourceError.ReadError
import quasar.evaluate.{FederatedQuery, QueryFederation}
import quasar.fs.Planner.PlannerErrorME
import quasar.mimir._, MimirCake._

import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{~>, \/, Monad}
import scalaz.concurrent.Task

final class MimirQueryFederation[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: PlannerErrorME](
    P: Cake,
    liftTask: Task ~> F)
    extends QueryFederation[T, F, QueryAssociate[T, F, Task], Stream[Task, Data]] {

  private val qscriptEvaluator =
    MimirQScriptEvaluator[T, F](P, liftTask)

  def evaluateFederated(q: FederatedQuery[T, QueryAssociate[T, F, Task]]): F[ReadError \/ Stream[Task, Data]] =
    qscriptEvaluator.evaluate(q.query).run(q.sources)
}
