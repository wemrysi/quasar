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

import argonaut.{Argonaut, Json}, Argonaut._

import quasar.Condition
import quasar.api.destination.{Destination, DestinationColumn, ResultSink, TypeCoercion}
import quasar.api.QueryEvaluator
import quasar.api.destination.ResultType
import quasar.api.push.{PushMeta, ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.table.{ColumnType, TableRef}

import java.time.Instant
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.data.{EitherT, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Timer}
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}

import shims.showToCats

final class DefaultResultPush[
    F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[F, Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, (D, T), Nothing],
    render: ResultRender[F, R],
    pushStatus: JMap[D, JMap[T, PushMeta]])
    extends ResultPush[F, T, D, Json] {

  import ResultPushError._

  def coerce(
      destinationId: D,
      tpe: ColumnType.Scalar)
      : F[Either[ResultPushError.DestinationNotFound[D], Json]] = {

    val destF = EitherT.fromOptionF[F, ResultPushError.DestinationNotFound[D], Destination[F]](
      lookupDestination(destinationId),
      ResultPushError.DestinationNotFound(destinationId))

    val jsonF = destF map { dest =>
      import dest._
      dest.coerce(tpe).asJson
    }

    jsonF.value
  }

  def start(
      tableId: T,
      destinationId: D,
      columns: List[DestinationColumn[Json]],
      path: ResourcePath,
      format: ResultType,
      limit: Option[Long])
      : F[Condition[ResultPushError[T, D]]] = {

    type RPE = ResultPushError[T, D]

    val writing = for {
      dest <- EitherT.fromOptionF[F, RPE, Destination[F]](
        lookupDestination(destinationId),
        ResultPushError.DestinationNotFound(destinationId))

      tableRef <- EitherT.fromOptionF[F, RPE, TableRef[Q]](
        lookupTable(tableId),
        ResultPushError.TableNotFound(tableId))

      sink <- format match {
        case ResultType.Csv =>
          EitherT.fromOptionF[F, RPE, ResultSink.Csv[F, dest.Type]](
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
                case Left(e) =>
                  val (c, p) = e.value
                  c(p)

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

      now <- EitherT.right[RPE](instantNow)
      submitted <- EitherT.right[RPE](jobManager.submit(Job((destinationId, tableId), sinked)))

      _ <- EitherT.cond[F](
        submitted,
        (),
        ResultPushError.PushAlreadyRunning(tableId, destinationId))

      pushMeta = PushMeta(path, format, limit, Status.running(now))

      _ <- EitherT.right[RPE](Concurrent[F] delay {
        pushStatus
          .computeIfAbsent(destinationId, _ => new ConcurrentHashMap[T, PushMeta]())
          .put(tableId, pushMeta)
      })
    } yield ()

    writing.value.map(Condition.eitherIso.reverseGet(_))
  }

  def cancel(tableId: T, destinationId: D): F[Condition[ExistentialError[T, D]]] = {
    val doCancel: F[Unit] = for {
      result <- jobManager.cancel((destinationId, tableId))

      now <- instantNow

      _ <- Concurrent[F] delay {
        pushStatus.computeIfPresent(destinationId, (_, mm) => {
          mm.computeIfPresent(tableId, {
            case (_, pm @ PushMeta(_, _, _, Status.Running(startedAt))) =>
              pm.copy(status = Status.canceled(startedAt, now))
            case (_, pm) =>
              pm
          })

          mm
        })
      }
    } yield result

    ensureBothExist[ExistentialError[T, D]](destinationId, tableId)
      .semiflatMap(_ => doCancel)
      .value
      .map(Condition.eitherIso.reverseGet(_))
  }

  def destinationStatus(destinationId: D): F[Either[DestinationNotFound[D], Map[T, PushMeta]]] =
    ensureDestinationExists[DestinationNotFound[D]](destinationId)
      .semiflatMap(_ => Concurrent[F] delay {
        Option(pushStatus.get(destinationId))
          .fold(Map[T, PushMeta]())(_.asScala.toMap)
      })
      .value

  def cancelAll: F[Unit] =
    jobManager.jobIds.flatMap(_.traverse_(jobManager.cancel(_)))


  ////

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def findCsvSink[T](sinks: NonEmptyList[ResultSink[F, T]]): Option[ResultSink.Csv[F, T]] =
    sinks collectFirstSome {
      case csvSink @ ResultSink.Csv(_, _) => Some(csvSink)
      case _ => None
    }

  private def ensureBothExist[E >: ExistentialError[T, D] <: ResultPushError[T, D]](
      destinationId: D,
      tableId: T)
      : EitherT[F, E, Unit] =
    ensureDestinationExists[E](destinationId) *> ensureTableExists[E](tableId)

  private def ensureDestinationExists[E >: DestinationNotFound[D] <: ResultPushError[T, D]](
      destinationId: D)
      : EitherT[F, E, Unit] =
    OptionT(lookupDestination(destinationId))
      .toRight[E](ResultPushError.DestinationNotFound(destinationId))
      .void

  private def ensureTableExists[E >: TableNotFound[T] <: ResultPushError[T, D]](
      tableId: T)
      : EitherT[F, E, Unit] =
    OptionT(lookupTable(tableId))
      .toRight[E](ResultPushError.TableNotFound(tableId))
      .void
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
      lookupTable: T => F[Option[TableRef[Q]]],
      evaluator: QueryEvaluator[F, Q, Stream[F, R]],
      lookupDestination: D => F[Option[Destination[F]]],
      jobManager: JobManager[F, (D, T), Nothing],
      render: ResultRender[F, R])
      : F[DefaultResultPush[F, T, D, Q, R]] =
    for {
      pushStatus <- Concurrent[F].delay(new ConcurrentHashMap[D, JMap[T, PushMeta]]())

      // we can't keep track of Completed and Failed jobs in this impl, so we consume them from JobManager
      // and update internal state accordingly
      _ <- Concurrent[F].start((jobManager.events evalMap {
        case JobEvent.Completed((di, ti), start, duration) =>
          Concurrent[F].delay(
            pushStatus.computeIfPresent(di, (_, mm) => {
              mm.computeIfPresent(ti, {
                case (_, pm) =>
                  pm.copy(status = Status.finished(
                    epochToInstant(start.epoch),
                    epochToInstant(start.epoch + duration)))
              })

              mm
            }))

        case JobEvent.Failed((di, ti), start, duration, ex) =>
          Concurrent[F].delay(
            pushStatus.computeIfPresent(di, (_, mm) => {
              mm.computeIfPresent(ti, {
                case (_, pm) =>
                  pm.copy(status = Status.failed(
                    ex,
                    epochToInstant(start.epoch),
                    epochToInstant(start.epoch + duration)))
              })

              mm
            }))
      }).compile.drain)
    } yield new DefaultResultPush(lookupTable, evaluator, lookupDestination, jobManager, render, pushStatus)

  private def epochToInstant(e: FiniteDuration): Instant =
    Instant.ofEpochMilli(e.toMillis)
}
