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
import quasar.api.resource.ResourcePath
import quasar.api.QueryEvaluator
import quasar.api.destination.Destinations
import quasar.api.table.Tables
import quasar.api.push.ResultPush
import quasar.api.push.ResultPushError

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import fs2.job.JobManager
import scalaz.\/

abstract class DefaultResultPush[
  F[_]: Concurrent: Timer, TableId, DestinationId, DestinationConfig, Query, TableData, TableSchema] private (
    tables: Tables[F, TableId, Query, TableData, TableSchema],
    evaluator: QueryEvaluator[F, Query, TableData],
    destinations: Destinations[F, Stream[F, ?], DestinationId, DestinationConfig],
    jobManager: JobManager[F, TableId, Nothing])
    extends ResultPush[F, TableId, DestinationId] {
  import ResultPushError._

  def start(tableId: TableId, destinationId: DestinationId, path: ResourcePath)
      : F[ExistentialError[TableId, DestinationId] \/ Condition[Exception]]

  def cancel(tableId: TableId): F[ExistentialError[TableId, DestinationId] \/ Condition[Exception]]

  def status(tableId: TableId): F[ExistentialError[TableId, DestinationId] \/ Condition[Exception]]

  def cancelAll: F[Condition[Exception]]
}
