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

import slamdata.Predef.Option
import quasar.{Data, RenderTreeT}
import quasar.api.ResourceError.ReadError
import quasar.contrib.pathy.AFile
import quasar.evaluate.{FederatedQuery, QueryFederation, Source}
import quasar.fp.liftMT
import quasar.fs.Planner.PlannerErrorME
import quasar.higher.HFunctor
import quasar.mimir._, MimirCake._

import fs2.Stream
import fs2.interop.scalaz._
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{~>, \/, DList, Monad, WriterT}
import scalaz.concurrent.Task
import scalaz.std.tuple._
import scalaz.syntax.traverse._

final class MimirQueryFederation[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Monad: PlannerErrorME] private (
    P: Cake,
    liftTask: Task ~> F)
    extends QueryFederation[T, F, QueryAssociate[T, F, Task], Stream[Task, Data]] {

  type FinalizersT[X[_], A] = WriterT[X, Finalizers[Task], A]

  private val qscriptEvaluator =
    MimirQScriptEvaluator[T, WriterT[F, Finalizers[Task], ?]](P, liftMT[F, FinalizersT] compose liftTask)

  def evaluateFederated(q: FederatedQuery[T, QueryAssociate[T, F, Task]]): F[ReadError \/ Stream[Task, Data]] = {
    val finalize: ((DList[Task[Unit]], Stream[Task, Data])) => Stream[Task, Data] = {
      case (fs, s) => fs.foldLeft(s)(_ onFinalize _)
    }

    val srcs: AFile => Option[Source[QueryAssociate[T, FinalizersT[F, ?], Task]]] =
      q.sources.andThen(_.map(_.map(HFunctor[QueryAssociate[T, ?[_], Task]].hmap(_)(liftMT[F, FinalizersT]))))

    qscriptEvaluator
      .evaluate(q.query)
      .run(srcs)
      .run
      .map(_.sequence map finalize)
  }
}

object MimirQueryFederation {
  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: Monad: PlannerErrorME](
      P: Cake,
      liftTask: Task ~> F)
      : QueryFederation[T, F, QueryAssociate[T, F, Task], Stream[Task, Data]] =
    new MimirQueryFederation[T, F](P, liftTask)
}
