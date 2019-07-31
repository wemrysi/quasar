/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.push

import slamdata.Predef._

import quasar.Condition
import quasar.connector.{Destination, ResultSink}
import quasar.api.QueryEvaluator
import quasar.api.destination.{ResultFormat, ResultType}
import quasar.api.push.ResultPush
import quasar.api.push.ResultPushError
import quasar.api.resource.ResourcePath
import quasar.api.table.{Tables, TableRef}
import quasar.impl.destinations.DestinationManager

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import fs2.job.JobManager
import scalaz.std.option._
import scalaz.syntax.equal._
import scalaz.syntax.applicative._
import scalaz.syntax.unzip._
import scalaz.{\/, \/-, Applicative, EitherT, Functor, NonEmptyList, OptionT, Traverse}
import shims._

abstract class DefaultResultPush[
  F[_]: Concurrent: Timer, TableId, DestinationId, DestinationConfig, Query, Result, TableSchema] private (
    tables: Tables[F, TableId, Query, Result, TableSchema],
    evaluator: QueryEvaluator[F, Query, Result],
    destManager: DestinationManager[DestinationId, DestinationConfig, F],
    jobManager: JobManager[F, TableId, Nothing],
    convertToFormat: (Result, ResultType[F]) => Stream[F, Byte])
    extends ResultPush[F, TableId, DestinationId] {
  import ResultPushError._

  def start(tableId: TableId, destinationId: DestinationId, path: ResourcePath, format: ResultType[F], limit: Option[Long])
      : F[ResultPushError[TableId, DestinationId] \/ Condition[Exception]] = {

    for {
      dest <- liftOptionF[F, ResultPushError[TableId, DestinationId], Destination[F]](
        destManager.destinationOf(destinationId),
        ResultPushError.DestinationNotFound(destinationId))

      tableRef <- liftOptionF[F, ResultPushError[TableId, DestinationId], TableRef[Query]](
        tables.table(tableId).map(_.toOption),
        ResultPushError.TableNotFound(tableId))

      query = tableRef.query
      columns = tableRef.columns

      evaluated = evaluator.evaluate(query).map(convertToFormat(_, format))

    } yield evaluated

    Concurrent[F].pure(\/-(Condition.normal[Exception]()))
  }

  def cancel(tableId: TableId): F[ExistentialError[TableId, DestinationId] \/ Condition[Exception]]

  def status(tableId: TableId): F[ExistentialError[TableId, DestinationId] \/ Condition[Exception]]

  def cancelAll: F[Condition[Exception]]

  private def liftOptionF[F[_]: Functor, E, A](oa: F[Option[A]], err: E): EitherT[F, E, A] =
    OptionT(oa).toRight[E](err)
}
