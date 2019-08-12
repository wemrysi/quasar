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
import quasar.api.push.{ResultPush, ResultPushError, Status}
import quasar.api.resource.ResourcePath
import quasar.api.table.TableRef
import scala.concurrent.duration._

import cats.effect.{Concurrent, Timer}
import fs2.Stream
import fs2.job.{JobManager, Job, Status => JobStatus}
import scalaz.std.option._
import scalaz.std.list._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.{EitherT, Functor, Id, NonEmptyList, OptionT, Traverse, \/}
import shims._

class DefaultResultPush[
  F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, R],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    convertToCsv: R => Stream[F, Byte])
    extends ResultPush[F, T, D] {
  import ResultPushError._

  def start(tableId: T, destinationId: D, path: ResourcePath, format: ResultType[F], limit: Option[Long])
      : F[Condition[ResultPushError[T, D]]] = {

    val writing = for {
      dest <- liftOptionF[F, ResultPushError[T, D], Destination[F]](
        lookupDestination(destinationId),
        ResultPushError.DestinationNotFound(destinationId))

      tableRef <- liftOptionF[F, ResultPushError[T, D], TableRef[Q]](
        lookupTable(tableId),
        ResultPushError.TableNotFound(tableId))

      sink: ResultSink.Aux[F, ResultType.Csv[F]] <-
        liftOptionF[F, ResultPushError[T, D], ResultSink.Aux[F, ResultType.Csv[F]]](
          findCsvSink(dest.sinks).point[F],
          ResultPushError.FormatNotSupported(destinationId, ResultFormat.fromResultType(format)))

      query = tableRef.query
      columns = tableRef.columns

      evaluated <- EitherT.rightT(evaluator.evaluate(query).map(convertToCsv))
      sinked = Stream.eval(sink(path, (columns, evaluated))).map(Right(_))

      _ <- EitherT.rightT(jobManager.submit(Job(tableId, sinked)))

    } yield ()

    writing.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def cancel(tableId: T): F[Condition[ExistentialError[T, D]]] =
    (ensureTableExists(tableId) *> (EitherT.rightT(jobManager.cancel(tableId))))
      .run.map(Condition.disjunctionIso.reverseGet(_))

  def status(tableId: T): F[ResultPushError[T, D] \/ Status] =
    (ensureTableExists(tableId) *> EitherT.rightT(jobManager.status(tableId))).fold(_.left, {
      case Some(JobStatus.Running | JobStatus.Pending) =>
        Status.Started.right[ResultPushError[T, D]]
      case Some(JobStatus.Canceled) =>
        Status.Canceled.right[ResultPushError[T, D]]
      case None =>
        ResultPushError.PushNotRunning(tableId).left[Status]
    })

  def cancelAll: F[Unit] =
    jobManager.jobIds
      .flatMap(Traverse[List].traverse(_)(jobManager.cancel(_))).void

  private def findCsvSink(sinks: NonEmptyList[ResultSink[F]]): Option[ResultSink.Aux[F, ResultType.Csv[F]]] =
    sinks.findMapM[Id.Id, ResultSink.Aux[F, ResultType.Csv[F]]] {
      case ResultSink.Csv(sink) => sink.some
      case _ => none
    }

  private def ensureTableExists(tableId: T): EitherT[F, ExistentialError[T, D], Unit] =
    OptionT(lookupTable(tableId))
      .toRight[ExistentialError[T, D]](ResultPushError.TableNotFound(tableId))
      .void

  private def liftOptionF[F[_]: Functor, E, A](oa: F[Option[A]], err: E): EitherT[F, E, A] =
    OptionT(oa).toRight[E](err)
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, R],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    convertToCsv: R => Stream[F, Byte]): DefaultResultPush[F, T, D, Q, R] =
    new DefaultResultPush(lookupTable, evaluator, lookupDestination, jobManager, convertToCsv)
}
