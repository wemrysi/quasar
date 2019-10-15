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

import slamdata.Predef.{Map => _, _}

import argonaut.{Argonaut, Json}, Argonaut._

import quasar.Condition
import quasar.api.destination.{Destination, DestinationColumn, ResultSink, TypeCoercion}
import quasar.api.QueryEvaluator
import quasar.api.destination.ResultType
import quasar.api.push.{PushMeta, ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.table.{ColumnType, TableRef}
import quasar.contrib.cats.foldable._

import java.time.Instant
import java.util.Map
import java.util.concurrent.ConcurrentHashMap

import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.Functor
import cats.data.{EitherT, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}

import scalaz.\/

import shims._

final class DefaultResultPush[
    F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    render: ResultRender[F, R],
    pushStatus: Map[T, PushMeta[D]])
    extends ResultPush[F, T, D, Json] {

  import ResultPushError._

  def coerce(
      destinationId: D,
      tpe: ColumnType)
      : F[ResultPushError.DestinationNotFound[D] \/ Json] = {

    val destF = liftOptionF[F, ResultPushError.DestinationNotFound[D], Destination[F]](
      lookupDestination(destinationId),
      ResultPushError.DestinationNotFound(destinationId))

    val jsonF = destF map { dest =>
      import dest._
      dest.coerce(tpe).asJson
    }

    jsonF.value.map(_.asScalaz)
  }

  def start(
      tableId: T,
      columns: List[DestinationColumn[Json]], // I hate seeing Json here, but... the Destination itself drives the decoding
      destinationId: D,
      path: ResourcePath,
      format: ResultType,
      limit: Option[Long])
      : F[Condition[ResultPushError[T, D]]] = {

    val writing = for {
      dest <- liftOptionF[F, ResultPushError[T, D], Destination[F]](
        lookupDestination(destinationId),
        ResultPushError.DestinationNotFound(destinationId))

      tableRef <- liftOptionF[F, ResultPushError[T, D], TableRef[Q]](
        lookupTable(tableId),
        ResultPushError.TableNotFound(tableId))

      sink <- format match {
        case ResultType.Csv =>
          liftOptionF[F, ResultPushError[T, D], ResultSink.Csv[F, dest.Type]](
            findCsvSink(dest.sinks).pure[F],
            ResultPushError.FormatNotSupported(destinationId, format.show))
      }

      columns <- {
        import dest._

        val validatedColumns = columns.traverse(
          _.traverse(
            _.as(
              TypeCoercion.appliedDecodeJson[Constructor, Type]).toEither).toValidatedNel)

        validatedColumns.fold(
          es =>
            EitherT.leftT[F, List[DestinationColumn[Type]]](
              ResultPushError.DestinationTypesNotDecodable(destinationId, es.map(_._1)): ResultPushError[T, D]),
          { cs =>
            // see? isn't this elegant?
            val applied = cs map { dc =>
              dc map {
                case Left((c, p)) => c(p)
                case Right(t) => t
              }
            }

            EitherT.rightT[F, ResultPushError[T, D]](applied)
          })
      }

      evaluated = format match {
        case ResultType.Csv =>
          evaluator.evaluate(tableRef.query)
            .map(_.flatMap(render.renderCsv(_, tableRef.columns, sink.config, limit)))
      }

      sinked = sink.run(path, columns, Stream.force(evaluated)).map(Right(_))

      now <- EitherT.right[ResultPushError[T, D]](instantNow)
      submitted <- EitherT.right[ResultPushError[T, D]](jobManager.submit(Job(tableId, sinked)))

      _ <- if (submitted)
        EitherT.rightT[F, ResultPushError[T, D]](())
      else
        EitherT.leftT[F, Unit](ResultPushError.PushAlreadyRunning(tableId): ResultPushError[T, D])

      pushMeta = PushMeta(destinationId, path, format, Status.running(now), limit)

      _ <- EitherT rightT[F, ResultPushError[T, D]] {
        Concurrent[F].delay(pushStatus.put(tableId, pushMeta))
      }
    } yield ()

    writing.value.map(e => Condition.disjunctionIso.reverseGet(e.asScalaz))
  }

  def cancel(tableId: T): F[Condition[ExistentialError[T, D]]] =
    (for {
      _ <- ensureTableExists[ExistentialError[T, D]](tableId)
      result <- EitherT.right[ExistentialError[T, D]](jobManager.cancel(tableId))
      now <- EitherT.right[ExistentialError[T, D]](instantNow)
      statusUpdate = Concurrent[F].delay(pushStatus.computeIfPresent(tableId, {
        case (_, pm @ PushMeta(_, _, _, Status.Running(startedAt), _)) =>
          pm.copy(status = Status.canceled(startedAt, now))
        case (_, pm) =>
          pm
      }))
      _ <- EitherT.right[ExistentialError[T, D]](statusUpdate)
    } yield result).value.map(e => Condition.disjunctionIso.reverseGet(e.asScalaz))

  def status(tableId: T): F[TableNotFound[T] \/ Option[PushMeta[D]]] =
    (ensureTableExists[TableNotFound[T]](tableId) *>
      EitherT.right(Concurrent[F].delay(Option(pushStatus.get(tableId))))).value.map(_.asScalaz)

  def cancelAll: F[Unit] =
    jobManager.jobIds.flatMap(_.traverse(jobManager.cancel(_))).void

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def findCsvSink[T](sinks: NonEmptyList[ResultSink[F, T]]): Option[ResultSink.Csv[F, T]] =
    sinks findMap {
      case csvSink @ ResultSink.Csv(_, _) => csvSink.some
      case _ => none
    }

  private def ensureTableExists[E >: TableNotFound[T] <: ResultPushError[T, D]](tableId: T)
      : EitherT[F, E, Unit] =
    OptionT(lookupTable(tableId))
      .toRight[E](ResultPushError.TableNotFound(tableId))
      .void

  private def liftOptionF[F[_]: Functor, E, A](oa: F[Option[A]], err: E): EitherT[F, E, A] =
    OptionT(oa).toRight[E](err)
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, T, Nothing],
    render: ResultRender[F, R]
  ): F[DefaultResultPush[F, T, D, Q, R]] = {
    for {
      pushStatus <- Concurrent[F].delay(new ConcurrentHashMap[T, PushMeta[D]]())
      // we can't keep track of Completed and Failed jobs in this impl, so we consume them from JobManager
      // and update internal state accordingly
      _ <- Concurrent[F].start((jobManager.events.evalMap {
        case JobEvent.Completed(ti, start, duration) => {
          Concurrent[F].delay(
            pushStatus.computeIfPresent(ti, {
              case (_, pm) =>
                pm.copy(status = Status.finished(
                  epochToInstant(start.epoch),
                  epochToInstant(start.epoch + duration)))
            }))
        }

        case JobEvent.Failed(ti, start, duration, ex) =>
          Concurrent[F].delay(
            pushStatus.computeIfPresent(ti, {
              case (_, pm) =>
                pm.copy(status = Status.failed(
                  ex,
                  epochToInstant(start.epoch),
                  epochToInstant(start.epoch + duration)))
            }))
      }).compile.drain)
    } yield new DefaultResultPush(lookupTable, evaluator, lookupDestination, jobManager, render, pushStatus)
  }

  private def epochToInstant(e: FiniteDuration): Instant =
    Instant.ofEpochMilli(e.toMillis)
}
