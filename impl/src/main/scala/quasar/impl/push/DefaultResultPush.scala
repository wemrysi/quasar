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
import quasar.impl.storage.{IndexedStore, PrefixStore}

import java.time.Instant
import java.util.{Map => JMap, Comparator}
import java.util.concurrent.{ConcurrentMap, ConcurrentNavigableMap, ConcurrentSkipListMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.Applicative
import cats.data.{Const, EitherT, Ior, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.effect.implicits._
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent}

import shapeless._

import skolems.∃

// TODO: do we want to use the upsert sink to perform full loads? or should we fail? I guess we don't have
//       ids and columns to use, so probably just fail?
//
// now that we're persisting, what do we do about tables that
// are deleted/archived? they'll show up in push output. guess it is up to FE to filter them?
// can also filter them in sdbe, don't want to bake in the name hacks here
//
// pending/running <- ephemeral, used to prevent duplicates
//
// when do we persist the push?
//

// may need an indexed semaphore for safe updates.

// if we store (push, terminal, maybeoffset), we would need to have
// store (push, terminal) and offset separately
//
// hm, or what about (push, maybeterminal, maybeoffset)?

// running status <- overrides terminal
// partial offset state
// terminal status

// ideally
//
// semantics, starting a new push invalidates any state of the previous, regardless of whether new finishes.
//
// accept the job, inmem: accepted
// jm runs the stream
// persist offsets as needed
// persist terminal state
//
// what happens if previous was also incremental and we want to restart
//
// maintain an additional id on offsets (d :: t :: uuid :: HNil, and store the UUID in the push store,
// then we can invalidate the previous (d :: t) when the first offset for the new one is persisted
//
// why not have resume also work for full pushes? just do not-incremental for now, if we ever want to enable it,
// we can.
//
// ok, so two stores, d :: t -> PushState[O] and d :: t :: id -> Offset
// can't really delete old offsets, somehow prune them? sigh
//
// start/re
// modernize? refresh? update?
//
//

// FIXME: Make a pass through `start` with an eye towards cancellation/failure and ensuring
//        state is correct in their presence.

// TODO[clustering]: Need to decide how we'll ensure a job is only ever running on a single node.
final class DefaultResultPush[
    F[_]: Concurrent: Timer, T, D, Q, R] private (
    lookupTable: T => F[Option[TableRef[Q]]],
    lookupDestination: D => F[Option[Destination[F]]],
    evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), Stream[F, R]],
    jobManager: JobManager[F, D :: T :: HNil, Nothing],
    render: ResultRender[F, R],
    active: ConcurrentNavigableMap[D :: Option[T] :: HNil, Status.Active],
    terminated: PrefixStore[F, D :: T :: HNil, Status.Terminal],
    pushes: PrefixStore[F, D :: T :: HNil, ∃[Push[?, Q]]],
    offsets: IndexedStore[F, D :: T :: HNil, ∃[OffsetKey.Actual]])
    extends ResultPush[F, T, D, Q] {

  import DefaultResultPush.liftKey
  import ResultPushError._

  def coerce(destinationId: D, tpe: ColumnType.Scalar)
      : F[Either[ResultPushError.DestinationNotFound[D], TypeCoercion[CoercedType]]] = {

    val destF = EitherT.fromOptionF[F, ResultPushError.DestinationNotFound[D], Destination[F]](
      lookupDestination(destinationId),
      DestinationNotFound(destinationId))

    destF.map(coerceWith(_, tpe)).value
  }

  def start(
      tableId: T,
      destinationId: D,
      push: ∃[Push[?, Q]],
      limit: Option[Long])
      : F[Condition[NonEmptyList[ResultPushError[T, D]]]] = {

    type Errs = NonEmptyList[ResultPushError[T, D]]

    def err(rpe: ResultPushError[T, D]): Errs = NonEmptyList.one(rpe)

    def handleFull(
        dest: Destination[F])(
        path: ResourcePath,
        query: Q,
        outputColumns: NonEmptyList[Column[(ColumnType.Scalar, dest.Type)]])
        : EitherT[F, Errs, Stream[F, Unit]] =
      /*

      evaluated =
        Stream.resource(evaluator(tableRef.query))
          .flatten
          .flatMap(render.render(_, tableRef.columns, sink.config, limit))

      sinked = sink.consume(path, typedColumns, evaluated).map(Right(_))

      for {
        ResultSink.CreateSink(renderCfg, consume) <-
          EitherT.fromEither[F] {
            createSink(dest).toRight[ResultPushError[T, D]](FullNotSupported(destinationId))
          }
      }*/
      ???

    def handleIncremental[A](
        dest: Destination[F])(
        path: ResourcePath,
        query: Q,
        outputColumns: NonEmptyList[Column[(ColumnType.Scalar, dest.Type)]],
        resumeConfig: ResumeConfig[A],
        initialOffset: Option[OffsetKey.Actual[A]])
        : EitherT[F, Errs, Stream[F, OffsetKey.Actual[A]]] =
      ???

    val writing = for {
      dest <- EitherT.fromOptionF[F, Errs, Destination[F]](
        lookupDestination(destinationId),
        err(DestinationNotFound(destinationId)))

      tableRef <- EitherT.fromOptionF[F, Errs, TableRef[Q]](
        lookupTable(tableId),
        err(TableNotFound(tableId)))

      key = destinationId :: tableId :: HNil

      _ <- EitherT(Sync[F].delay(active.containsKey(liftKey(key))) map {
        case true => Left(err(PushAlreadyRunning(tableId, destinationId)))
        case false => Right(())
      })

      typedOutputColumns <- EitherT.fromEither[F] {
        push.value.columns
          .traverse(typedColumn(destinationId, dest, _).toValidatedNel)
          .toEither
      }

      sinked <- push.value match {
        case Push.Full(path, q, _) =>
          handleFull(dest)(path, q, typedOutputColumns)

        case Push.Incremental(path, q, _, resumeCfg, initOffset) =>
          handleIncremental(dest)(path, q, typedOutputColumns, resumeCfg, initOffset)
            .map(_.evalMap(o => offsets.insert(key, ∃(o))))
      }

      acceptedAt <- EitherT.right[Errs](instantNow)
      accepted = Status.Accepted(acceptedAt, limit)

      alreadyRunning <- EitherT.right[Errs](Sync[F] delay {
        Option(active.putIfAbsent(liftKey(key), accepted)).isDefined
      })

      _ <- if (alreadyRunning)
        EitherT.leftT[F, Unit](err(PushAlreadyRunning(tableId, destinationId)))
      else
        EitherT.right[Errs](pushes.insert(key, push) *> offsets.delete(key))
      })

      setRunning = instantNow.flatMap(ts => Sync[F] delay {
        active.replace(liftKey(key), accepted, Status.Running(ts, limit))
      })

      job = Stream.eval_(setRunning) ++ sinked

      submitted <- EitherT.right[Errs](jobManager.submit(Job(key, job.map(Right(_)))))

      _ <- if (submitted)
        EitherT.rightT[F, Errs](())
      else
        // TODO[logging]: this shouldn't happen, so should log if it ever does
        EitherT.left[Unit](
          Sync[F].delay(active.remove(liftKey(key), accepted))
            .as(err(PushAlreadyRunning(tableId, destinationId))))
    } yield ()

    writing.value.map(Condition.eitherIso.reverseGet(_))
  }

  def cancel(tableId: T, destinationId: D): F[Condition[ExistentialError[T, D]]] = {
    val doCancel: F[Unit] = for {
      result <- jobManager.cancel(destinationId :: tableId :: HNil)

      now <- instantNow

      _ <- Concurrent[F] delay {
        pushStatus.computeIfPresent(destinationId, (_, mm) => {
          mm.computeIfPresent(tableId, {
            case (_, pm @ PushMeta(_, _, _, Status.Running(startedAt))) =>
              pm.copy(status = Status.Canceled(startedAt, now))
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

  // FIXME
  def resume(
      tableId: T,
      destinationId: D,
      limit: Option[Long])
      : F[Condition[ResultPushError[T, D]]] = ???

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

  private def coerceWith(dest: Destination[F], scalar: ColumnType.Scalar): TypeCoercion[CoercedType] = {
    import dest._

    dest.coerce(scalar) map { id =>
      val param = construct(id) match {
        case Right(e) =>
          val (_, p) = e.value
          Some(p.map(∃(_)))

        case Left(_) => None
      }

      CoercedType(Labeled(id.label, TypeIndex(typeIdOrdinal(id))), param)
    }
  }

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def validateCoercion(
      destId: D,
      dest: Destination[F],
      column: String,
      scalar: ColumnType.Scalar,
      selected: TypeIndex)
      : Either[ResultPushError[T, D], Unit] = {

    val isValid = coerceWith(dest, scalar) match {
      case TypeCoercion.Unsatisfied(_, top) =>
        top.exists(_.index.value === selected)

      case TypeCoercion.Satisfied(ts) =>
        ts.exists(_.index.value === selected)
    }

    if (isValid)
      Right(())
    else
      Left(InvalidCoercion(destId, column, scalar, selected))
  }

  private def constructType(destId: D, dest: Destination[F], name: String, selected: SelectedType)
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
            TypeConstructionFailed(destId, name, id.label, NonEmptyList.one(err)))
      }

      case None =>
        Left(TypeNotFound(destId, name, selected.index))
    }
  }

  private def typedColumn(destId: D, dest: Destination[F], column: Push.OutputColumn)
      : Either[ResultPushError[T, D], Column[(ColumnType.Scalar, dest.Type)]] =
    column traverse {
      case (scalar, selected) =>
        for {
          _ <- validateCoercion(destId, dest, column.name, scalar, selected.index)
          t <- constructType(destId, dest, column.name, selected)
        } yield (scalar, t)
    }

  private def createSink[A](sinks: NonEmptyList[ResultSink[F, A]]): Option[ResultSink.CreateSink[F, A]] =
    sinks collectFirst {
      case sink @ ResultSink.CreateSink(_, _) => sink
    }

  private def upsertSink[A](sinks: NonEmptyList[ResultSink[F, A]]): Option[ResultSink.UpsertSink[F, A]] =
    sinks collectFirst {
      case sink @ ResultSink.UpsertSink(_, _) => sink
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
      .toRight[E](DestinationNotFound(destinationId))
      .void

  private def ensureTableExists[E >: TableNotFound[T] <: ResultPushError[T, D]](
      tableId: T)
      : EitherT[F, E, Unit] =
    OptionT(lookupTable(tableId))
      .toRight[E](TableNotFound(tableId))
      .void
}

object DefaultResultPush {
  def apply[F[_]: Concurrent: Timer, T, D, Q, R](
      lookupTable: T => F[Option[TableRef[Q]]],
      lookupDestination: D => F[Option[Destination[F]]],
      evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), Stream[F, R]],
      jobManager: JobManager[F, D :: T :: HNil, Nothing],
      render: ResultRender[F, R],
      terminated: PrefixStore[F, D :: T :: HNil, Status.Terminal],
      pushes: PrefixStore[F, D :: T :: HNil, ∃[Push[?, Q]]],
      offsets: IndexedStore[F, D :: T :: HNil, ∃[OffsetKey.Actual]])
      : F[DefaultResultPush[F, T, D, Q, R]] = {

    def epochToInstant(e: FiniteDuration): Instant =
      Instant.ofEpochMilli(e.toMillis)

    def handleEvent(active: ConcurrentMap[D :: Option[T] :: HNil, Status.Active])
        : JobEvent[D :: T :: HNil] => F[Unit] = {

      case JobEvent.Completed(id, start, duration) =>
        Sync[F].delay(active.get(liftKey(id))) flatMap {
          case s @ Status.Running(_, limit) =>
            val finished = Status.Finished(
              epochToInstant(start.epoch),
              epochToInstant(start.epoch + duration),
              limit)

            terminated.insert(id, finished)
              .guarantee(Sync[F].delay(active.remove(liftKey(id), s)).void)

          case _ => Applicative[F].unit
        }

      case JobEvent.Failed(id, start, duration, ex) =>
        Sync[F].delay(active.get(liftKey(id))) flatMap {
          case s @ Status.Running(_, limit) =>
            val failed = Status.Failed(
              epochToInstant(start.epoch),
              epochToInstant(start.epoch + duration),
              limit,
              ex)

            terminated.insert(id, failed)
              .guarantee(Sync[F].delay(active.remove(liftKey(id), s)).void)

          case _ => Applicative[F].unit
        }
    }

    // FIXME: define this
    def prefixComparator: Comparator[D :: Option[T] :: HNil] =
      ???

    for {
      active <- Sync[F].delay(new ConcurrentSkipListMap[D :: Option[T] :: HNil, Status.Active](prefixComparator))
      _ <- Concurrent[F].start(jobManager.events.evalMap(handleEvent(active)).compile.drain)
    } yield {
      new DefaultResultPush(
        lookupTable,
        lookupDestination,
        evaluator,
        jobManager,
        render,
        active,
        terminated,
        pushes,
        offsets)
    }
  }

  private def liftKey[D, T](key: D :: T :: HNil): D :: Option[T] :: HNil =
    key.updateWith(Option(_: T))
}
