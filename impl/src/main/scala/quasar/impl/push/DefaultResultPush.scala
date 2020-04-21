/*
 * Copyright 2020 Precog Data
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

import slamdata.Predef.{Boolean => SBoolean, _}

import quasar.Condition
import quasar.api.{Column, ColumnType, Labeled, QueryEvaluator}
import quasar.api.Label.Syntax._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.table.TableRef
import quasar.connector.Offset
import quasar.connector.destination._
import quasar.connector.render.{ResultRender, RenderConfig}

import java.time.Instant
import java.util.{Map => JMap}
import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.data.{Const, EitherT, Ior, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}

import shims.showToCats

import skolems.∃

// hm, could use an in-memory mapdb directly
// TODO: hm, can we build a pure prefix store?
// start a push, does it happen immediately or
//
// now that we're persisting, what do we do about tables that
// are deleted/archived? they'll show up in push output.
//
// pending/running <- ephemeral, used to prevent duplicates
//
// terminated <- persistent
//
// also need to store the incremental state
//  - current offset
//  - ???
//
final class DefaultResultPush[
    F[_]: Concurrent: Timer, T, D, Q, R, O] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    evaluator: QueryEvaluator[Resource[F, ?], Q, Stream[F, R]],
    lookupDestination: D => F[Option[Destination[F]]],
    jobManager: JobManager[F, (D, T), Nothing],
    render: ResultRender[F, R],
    running: ConcurrentHashMap[(D, T), PushMeta[O]],
    terminated: PrefixStore[F, D :: T :: HNil, PushMeta[O]],
    offsets: IndexedStore[F, (D, T), Offset])
    extends ResultPush[F, T, D, O] {

  import ResultPushError._

  def coerce(
      destinationId: D,
      tpe: ColumnType.Scalar)
      : F[Either[ResultPushError.DestinationNotFound[D], TypeCoercion[CoercedType]]] = {

    val destF = EitherT.fromOptionF[F, ResultPushError.DestinationNotFound[D], Destination[F]](
      lookupDestination(destinationId),
      ResultPushError.DestinationNotFound(destinationId))

    val coerceF = destF map { dest =>
      import dest._

      dest.coerce(tpe) map { id =>
        val param = construct(id) match {
          case Right(e) =>
            val (_, p) = e.value
            Some(p.map(∃(_)))

          case Left(_) => None
        }

        CoercedType(Labeled(id.label, TypeIndex(typeIdOrdinal(id))), param)
      }
    }

    coerceF.value
  }

  def start(
      tableId: T,
      destinationId: D,
      push: Push[O],
      limit: Option[Long])
      : F[Condition[NonEmptyList[ResultPushError[T, D]]]] = {

    type Errs = NonEmptyList[ResultPushError[T, D]]

    def err(rpe: ResultPushError[T, D]): Errs = NonEmptyList.one(rpe)

    def constructType(dest: Destination[F], name: String, selected: SelectedType)
        : Either[ResultPushError[T, D], dest.Type] = {

      import dest._
      import ParamType._

      def checkBounds(b: Int Ior Int, i: Int): SBoolean =
        b.fold(_ <= i, _ >= i, (min, max) => min <= i && i <= max)

      typeIdOrdinal.getOption(selected.index.ordinal) match {
        case Some(id) => construct(id) match {
          case Left(t) => Right(t)

          case Right(e) =>
            val (c, Labeled(label, formal)) = e.value

            val back = for {
              actual <- selected.arg.toRight(ParamError.ParamMissing(label, formal))

              t <- (formal: Formal[A] forSome { type A }, actual) match {
                case (Boolean(_), ∃(Boolean(Const(b)))) =>
                  Right(c(b.asInstanceOf[e.A]))

                case (Integer(Integer.Args(None, None)), ∃(Integer(Const(i)))) =>
                  Right(c(i.asInstanceOf[e.A]))

                case (Integer(Integer.Args(Some(bounds), None)), ∃(Integer(Const(i)))) =>
                  if (checkBounds(bounds, i))
                    Right(c(i.asInstanceOf[e.A]))
                  else
                    Left(ParamError.IntOutOfBounds(label, i, bounds))

                case (Integer(Integer.Args(None, Some(step))), ∃(Integer(Const(i)))) =>
                  if (step(i))
                    Right(c(i.asInstanceOf[e.A]))
                  else
                    Left(ParamError.IntOutOfStep(label, i, step))

                case (Integer(Integer.Args(Some(bounds), Some(step))), ∃(Integer(Const(i)))) =>
                  if (!checkBounds(bounds, i))
                    Left(ParamError.IntOutOfBounds(label, i, bounds))
                  else if (!step(i))
                    Left(ParamError.IntOutOfStep(label, i, step))
                  else
                    Right(c(i.asInstanceOf[e.A]))

                case (Enum(possiblities), ∃(EnumSelect(Const(key)))) =>
                  possiblities.lookup(key)
                    .map(a => c(a.asInstanceOf[e.A]))
                    .toRight(ParamError.ValueNotInEnum(label, key, possiblities.keys))

                case _ => Left(ParamError.ParamMismatch(label, formal, actual))
              }
            } yield t

            back.leftMap(err =>
              ResultPushError.TypeConstructionFailed(destinationId, name, id.label, NonEmptyList.one(err)))
        }

        case None =>
          Left(ResultPushError.TypeNotFound(destinationId, name, selected.index))
      }
    }

    val writing = for {
      dest <- EitherT.fromOptionF[F, Errs, Destination[F]](
        lookupDestination(destinationId),
        err(ResultPushError.DestinationNotFound(destinationId)))

      tableRef <- EitherT.fromOptionF[F, Errs, TableRef[Q]](
        lookupTable(tableId),
        err(ResultPushError.TableNotFound(tableId)))

      sink <- format match {
        case ResultType.Csv =>
          EitherT.fromOptionF[F, Errs, ResultSink.CreateSink[F, dest.Type]](
            findCsvSink(dest.sinks).pure[F],
            err(ResultPushError.FormatNotSupported(destinationId, format.show)))
      }

      typedColumns <-
        EitherT.fromEither[F](
          columns.traverse(c => c.traverse(constructType(dest, c.name, _)).toValidatedNel).toEither)

      evaluated =
        Stream.resource(evaluator(tableRef.query))
          .flatten
          .flatMap(render.render(_, tableRef.columns, sink.config, limit))

      sinked = sink.consume(path, typedColumns, evaluated).map(Right(_))

      now <- EitherT.right[Errs](instantNow)

      currentMeta = PushMeta(path, format, limit, Status.running(now))

      // This needs to happen prior to submit to avoid the race condition where the submitted
      // job fails/completes before the initial meta is set
      prevMeta <- EitherT.right[Errs](Concurrent[F] delay {
        Option(pushStatus
          .computeIfAbsent(destinationId, _ => new ConcurrentHashMap[T, PushMeta]())
          .put(tableId, currentMeta))
      })

      submitted <- EitherT.right[Errs](jobManager.submit(Job((destinationId, tableId), sinked)))

      // If submit fails, we'll restore the previous status if nothing else changed it.
      restorePrev = Concurrent[F] delay {
        prevMeta match {
          case Some(pm) =>
            pushStatus.computeIfPresent(destinationId, (_, mm) => {
              mm.replace(tableId, currentMeta, pm)
              mm
            })

          case None =>
            pushStatus.computeIfPresent(destinationId, (_, mm) => {
              mm.remove(tableId, currentMeta)
              mm
            })
        }
      }

      _ <- if (submitted)
        EitherT.rightT[F, Errs](())
      else
        EitherT.left[Unit](restorePrev.as(err(
          ResultPushError.PushAlreadyRunning(tableId, destinationId))))

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

  def resume(
      tableId: TableId,
      destinationId: DestinationId,
      limit: Option[Long])
      : F[Condition[ResultPushError[TableId, DestinationId]]] = ???

  def destinationStatus(destinationId: D): F[Either[DestinationNotFound[D], Map[T, PushMeta]]] =
    ensureDestinationExists[DestinationNotFound[D]](destinationId)
      .semiflatMap(x => Concurrent[F] delay {
        val back = Option(pushStatus.get(destinationId))
        back.fold(Map[T, PushMeta]())(_.asScala.toMap)
      })
      .value

  def cancelAll: F[Unit] =
    jobManager.jobIds.flatMap(_.traverse_(jobManager.cancel(_)))


  ////

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def findCsvSink[T](sinks: NonEmptyList[ResultSink[F, T]]): Option[ResultSink.CreateSink[F, T]] =
    sinks collectFirstSome {
      case csvSink @ ResultSink.CreateSink(RenderConfig.Csv(_, _, _, _, _, _, _, _, _), _) => Some(csvSink)
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
      evaluator: QueryEvaluator[Resource[F, ?], Q, Stream[F, R]],
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
