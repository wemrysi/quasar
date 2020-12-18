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

import quasar.{Condition, Store}
import quasar.api.{Column, ColumnType, Labeled, QueryEvaluator}
import quasar.api.Label.Syntax._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.connector.Offset
import quasar.connector.destination._
import quasar.connector.render.{RenderInput, ResultRender}
import quasar.impl.storage.PrefixStore

import java.lang.IllegalStateException
import java.time.Instant
import java.util.Comparator
import java.util.concurrent.{ConcurrentMap, ConcurrentNavigableMap, ConcurrentSkipListMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.{Functor, Order, Show, Traverse}
import cats.data.{Const, EitherT, Ior, OptionT, NonEmptyList, ValidatedNel}
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.effect.concurrent.Deferred
import cats.effect.implicits._
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent, Timestamp => JobStarted}

import io.chrisdavenport.log4cats.Logger
import io.chrisdavenport.log4cats.slf4j.Slf4jLogger

import pathy.Path

import scodec.Codec

import shapeless._
import shapeless.HList.ListCompat._

import shims.orderToCats

import skolems.∃

// TODO[clustering]: Need to decide how we'll ensure a job is only ever running on a single node.
private[impl] final class DefaultResultPush[
    F[_]: Concurrent: Timer, D: Codec: Show, Q, R] private (
    lookupDestination: D => F[Option[Destination[F]]],
    evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), R],
    jobManager: JobManager[F, D :: ResourcePath :: HNil, Nothing],
    render: ResultRender[F, R],
    active: ConcurrentNavigableMap[D :: Option[ResourcePath] :: HNil, DefaultResultPush.ActiveState[F, Q]],
    pushes: PrefixStore.SCodec[F, D :: ResourcePath :: HNil, ∃[Push[?, Q]]],
    offsets: Store[F, D :: ResourcePath :: HNil, ∃[OffsetKey.Actual]],
    log: Logger[F])
    extends ResultPush[F, D, Q] {

  import DefaultResultPush.{ActiveState, debugKey, debugTerminal, liftKey}
  import ResultPushError._

  def cancel(destinationId: D, path: ResourcePath): F[Condition[DestinationNotFound[D]]] = {
    val doCancel: F[Unit] = for {
      canceledAt <- instantNow

      key = destinationId :: path :: HNil

      _ <- jobManager.cancel(key)

      // FIXME: Transition to "Cancelling"
      state <- Sync[F].delay(Option(active.remove(liftKey(key))))

      _ <- state traverse_ {
        case ActiveState.PushRunning(p, runningAt, _, limit, terminal) =>
          val canceled = Status.Canceled(runningAt, canceledAt, limit)
          pushes.insert(key, ∃[Push[?, Q]](p.value.copy(status = canceled)))
            .guarantee(terminal.complete(canceled))
            .productR(log.debug(debugTerminal("canceled", key, canceled)))

        case accepted =>
          val canceled = Status.Canceled(accepted.at, canceledAt, accepted.limit)
          accepted.terminal.complete(canceled)
            .productR(log.debug(debugTerminal("aborted", key, canceled)))
      }
    } yield ()

    destination[DestinationNotFound[D]](destinationId)
      .semiflatMap(_ => doCancel)
      .value
      .map(Condition.eitherIso.reverseGet(_))
  }

  def cancelAll: F[Unit] =
    Stream.eval(Sync[F].delay(active.keySet.iterator.asScala))
      .flatMap(Stream.fromIterator[F](_))
      .map(lowerKey)
      .parEvalMapUnordered(Int.MaxValue) {
        case d #: p #: HNil => cancel(d, p).attempt.void
      }
      .compile.drain

  def coerce(destinationId: D, scalar: ColumnType.Scalar)
      : F[Either[DestinationNotFound[D], TypeCoercion[CoercedType]]] =
    destination[DestinationNotFound[D]](destinationId)
      .map(coerceWith(_, scalar))
      .value

  def pushedTo(destinationId: D)
      : F[Either[DestinationNotFound[D], Map[ResourcePath, ∃[Push[?, Q]]]]] = {

    def overrideWithActive(m: Map[ResourcePath, ∃[Push[?, Q]]]): F[Map[ResourcePath, ∃[Push[?, Q]]]] = {
      val activeForDest = Sync[F] delay {
        active
          .subMap(
            destinationId :: Some(ResourcePath.root()) :: HNil,
            destinationId :: None :: HNil)
          .entrySet.iterator.asScala
      }

      Stream.eval(activeForDest)
        .flatMap(Stream.fromIterator[F](_))
        .map(e => (lowerKey(e.getKey).select[ResourcePath], e.getValue))
        .compile.fold(m) {
          case (acc, (path, ActiveState.StartAccepted(cfg, ts, lim, _))) =>
            acc.updated(path, ∃[Push[?, Q]](Push(cfg.value, ts, Status.Accepted(ts, lim))))

          case (acc, (path, ActiveState.UpdateAccepted(p, ts, _))) =>
            acc.updated(path, ∃[Push[?, Q]](p.value.copy(status = Status.Accepted(ts, None))))

          case (acc, (path, ActiveState.PushRunning(p, ts, _, lim, _))) =>
            acc.updated(path, ∃[Push[?, Q]](p.value.copy(status = Status.Running(ts, lim))))
        }
    }

    destination[DestinationNotFound[D]](destinationId)
      .semiflatMap(_ =>
        pushes.prefixedEntries(destinationId :: HNil)
          .map(_.leftMap(_.select[ResourcePath]))
          .compile.to(Map))
      .semiflatMap(overrideWithActive)
      .value
  }

  def start(destinationId: D, config: ∃[PushConfig[?, Q]], limit: Option[Long])
      : F[Either[NonEmptyList[ResultPushError[D]], F[Status.Terminal]]] = {

    val cfg = config.value
    val key = destinationId :: cfg.path :: HNil

    EitherT.right[Errs](instantNow)
      .flatMap(runPush(destinationId, cfg, limit, _, None))
      .productL(EitherT.right[Errs](log.debug(s"${debugKey(key)} Start push accepted")))
      .value
  }

  def update(destinationId: D, path: ResourcePath)
      : F[Either[NonEmptyList[ResultPushError[D]], F[Status.Terminal]]] = {

    val key = destinationId :: path :: HNil

    def resume[A](inc: PushConfig.Incremental[A, Q], createdAt: Instant)
        : Option[∃[OffsetKey.Actual]] => EitherT[F, Errs, F[Status.Terminal]] = {

      case Some(resumeFrom) =>
        val actualKey: OffsetKey.Actual[_] = resumeFrom.value
        val expectedKey: OffsetKey.Formal[Unit, A] = inc.resumeConfig.resultOffsetColumn.tpe

        (actualKey, expectedKey) match {
          case (realKey @ OffsetKey.RealKey(_), OffsetKey.RealKey(_)) =>
            runPush(destinationId, inc, None, createdAt, Some(realKey))

          case (stringKey @ OffsetKey.StringKey(_), OffsetKey.StringKey(_)) =>
            runPush(destinationId, inc, None, createdAt, Some(stringKey))

          case (dateTimeKey @ OffsetKey.DateTimeKey(_), OffsetKey.DateTimeKey(_)) =>
            runPush(destinationId, inc, None, createdAt, Some(dateTimeKey))

          case _ =>
            val ex = new IllegalStateException(
              s"Invalid offset, expected ${expectedKey.show}, found ${actualKey.show}")

            EitherT {
              log.error(ex)(s"${debugKey(key)} Unable to resume incremental push")
                .productR(Concurrent[F].raiseError[Either[Errs, F[Status.Terminal]]](ex))
            }
        }

      case None =>
        EitherT.right[Errs](log.warn(s"${debugKey(key)} Offset not found, updating from initial"))
          .productR(runPush(destinationId, inc, None, createdAt, None))
    }

    OptionT(pushes.lookup(key))
      .toRight(err(PushNotFound(destinationId, path)))
      .flatMap { push0 =>
        val push = push0.value

        push.config match {
          case full @ PushConfig.Full(_, _, _) =>
            runPush(destinationId, full, None, push.createdAt, None)

          case inc @ PushConfig.Incremental(_, _, _, _, _) =>
            EitherT.right[Errs](offsets.lookup(key))
              .flatMap(resume(inc, push.createdAt))
        }
      }
      .productL(EitherT.right[Errs](log.debug(s"${debugKey(key)} Update push accepted")))
      .value
  }


  ////

  private type Errs = NonEmptyList[ResultPushError[D]]

  private def err(rpe: ResultPushError[D]): Errs = NonEmptyList.one(rpe)

  private def runPush[A](
      destinationId: D,
      config: PushConfig[A, Q],
      limit: Option[Long],
      createdAt: Instant,
      resumeFrom: Option[OffsetKey.Actual[A]])
      : EitherT[F, Errs, F[Status.Terminal]] = {

    def handleFull(
        dest: Destination[F])(
        path: ResourcePath,
        query: Q,
        outputColumns: NonEmptyList[Column[(ColumnType.Scalar, dest.Type)]])
        : EitherT[F, Errs, Stream[F, Unit]] = {

      val createSink = dest.sinks collectFirst {
        case sink @ ResultSink.CreateSink(_) => sink
      }

      EitherT.fromOption[F](createSink, err(FullNotSupported(destinationId))) map { sink =>
        val (renderColumns, destColumns) =
          Functor[NonEmptyList].compose[Column].unzip(outputColumns)

        val (renderConfig, pipe) = sink.consume(path, destColumns)

        Stream.resource(evaluator((query, None)))
          .flatMap(render.render(_, renderColumns, renderConfig, limit))
          .through(pipe)
      }
    }

    def handleIncremental[A](
        dest: Destination[F])(
        path: ResourcePath,
        query: Q,
        nonIdOutputColumns: List[Column[(ColumnType.Scalar, dest.Type)]],
        resumeConfig: ResumeConfig[A],
        actualOffset: Either[Option[OffsetKey.Actual[A]], OffsetKey.Actual[A]])
        : EitherT[F, Errs, Stream[F, OffsetKey.Actual[A]]] = {

      val C = Functor[Column]

      val upsertSink = dest.sinks collectFirst {
        case sink @ ResultSink.UpsertSink(_) => sink
      }

      for {
        sink <- EitherT.fromOption[F](upsertSink, err(IncrementalNotSupported(destinationId)))

        (idColumn, _) = C.unzip(resumeConfig.resultIdColumn)

        idOutputColumn = resumeConfig.resultIdColumn.map(_.leftMap(IdType.scalarP(_)))

        (idRenderColumn, idDestColumn) <- EitherT.fromEither[F] {
          typedColumn(destinationId, dest, idOutputColumn)
            .bimap(err, C.unzip)
        }

        (nonIdRenderColumns, nonIdDestColumns) =
          Functor[List].compose[Column].unzip(nonIdOutputColumns)

        (offsetValue, isUpdate) = actualOffset match {
          case Left(initial) => (initial, false)
          case Right(resume) => (Some(resume), true)
        }

        offset = offsetValue.map(o => Offset(resumeConfig.sourceOffsetPath, ∃(o)))

        upsertArgs =
          ResultSink.UpsertSink.Args(
            path,
            idDestColumn,
            nonIdDestColumns,
            if (isUpdate) WriteMode.Append else WriteMode.Replace)

        (renderConfig, toOffsets) = sink.consume(upsertArgs)

        dataEvents =
          Stream.resource(evaluator((query, offset))) flatMap { results =>
            val input =
              if (isUpdate)
                RenderInput.Incremental(results)
              else
                RenderInput.Initial(results)

            render.renderUpserts(
              input,
              idColumn,
              resumeConfig.resultOffsetColumn,
              NonEmptyList(idRenderColumn, nonIdRenderColumns),
              renderConfig,
              limit)
          }

      } yield dataEvents.through(toOffsets[A])
    }

    for {
      dest <- destination(destinationId).leftMap(err)

      key = destinationId :: config.path :: HNil

      _ <- EitherT(Sync[F].delay(active.containsKey(liftKey(key))) map {
        case true => Left(err(PushAlreadyRunning(destinationId, config.path)))
        case false => Right(())
      })

      sinked <- config match {
        case PushConfig.Full(path, q, columns) =>
          EitherT.fromEither[F](typedColumns(destinationId, dest, columns))
            .flatMap(handleFull(dest)(path, q, _))

        case PushConfig.Incremental(path, q, otherCols, resumeCfg, initOffset) =>
          val offset = resumeFrom match {
            case Some(key) => Right(key)
            case None => Left(initOffset)
          }

          EitherT.fromEither[F](typedColumns(destinationId, dest, otherCols))
            .flatMap(handleIncremental(dest)(path, q, _, resumeCfg, offset))
            .map(_.evalMap(o => offsets.insert(key, ∃(o))))
      }

      terminal <- EitherT.right[Errs](Deferred[F, Status.Terminal])
      acceptedAt <- EitherT.right[Errs](instantNow)
      accepted = ActiveState.StartAccepted(∃[PushConfig[?, Q]](config), acceptedAt, limit, terminal)

      alreadyRunning <- EitherT.right[Errs](Sync[F] delay {
        Option(active.putIfAbsent(liftKey(key), accepted)).isDefined
      })

      _ <- if (alreadyRunning)
        EitherT.leftT[F, Unit](err(PushAlreadyRunning(destinationId, config.path)))
      else
        EitherT.rightT[F, Errs](())

      preamble = for {
        runningAt <- instantNow

        unknownPush = ∃[Push[?, Q]](Push(config, createdAt, Status.Unknown(runningAt, limit)))
        running = ActiveState.PushRunning(unknownPush, runningAt, acceptedAt, limit, terminal)

        isResume = resumeFrom.isDefined

        _ <- Sync[F].delay(active.replace(liftKey(key), accepted, running))
        _ <- pushes.insert(key, unknownPush)
        _ <- if (isResume) ().pure[F] else offsets.delete(key).void

        _ <- log.debug(s"${debugKey(key)} Push running")
      } yield ()

      job = Stream.eval_(preamble) ++ sinked

      submitted <- EitherT.right[Errs](jobManager.submit(Job(key, job.map(Right(_)))))

      _ <- if (submitted)
        EitherT.rightT[F, Errs](())
      else
        // This shouldn't happen
        EitherT.left[Unit](for {
          _ <- Sync[F].delay(active.remove(liftKey(key), accepted))
          canceledAt <- instantNow
          _ <- terminal.complete(Status.Canceled(acceptedAt, canceledAt, limit))
          _ <- log.error(s"${debugKey(key)} Conflict with job manager, already running")
        } yield err(PushAlreadyRunning(destinationId, config.path)))

    } yield terminal.get
  }

  private def coerceWith(dest: Destination[F], scalar: ColumnType.Scalar): TypeCoercion[CoercedType] = {
    import dest._

    dest.coerce(scalar) map { id =>
      val params = construct(id) match {
        case Right(Constructor.Unary(p1, _)) =>
          List(p1.map(∃(_)))

        case Right(Constructor.Binary(p1, p2, _)) =>
          List(p1, p2).map(_.map(∃(_)))

        case Right(Constructor.Ternary(p1, p2, p3, _)) =>
          List(p1, p2, p3).map(_.map(∃(_)))

        case Left(_) => List()
      }

      CoercedType(Labeled(id.label, TypeIndex(typeIdOrdinal(id))), params)
    }
  }

  private def instantNow: F[Instant] =
    Timer[F].clock.realTime(MILLISECONDS)
      .map(Instant.ofEpochMilli(_))

  private def typedColumns[G[_]: Traverse](
      destId: D,
      dest: Destination[F],
      columns: G[PushConfig.OutputColumn])
      : Either[NonEmptyList[ResultPushError[D]], G[Column[(ColumnType.Scalar, dest.Type)]]] =
    columns.traverse(typedColumn(destId, dest, _).toValidatedNel).toEither

  private def typedColumn(destId: D, dest: Destination[F], column: PushConfig.OutputColumn)
      : Either[ResultPushError[D], Column[(ColumnType.Scalar, dest.Type)]] =
    column traverse {
      case (scalar, selected) =>
        for {
          t <- constructType(destId, dest, column.name, selected)
          _ <- validateCoercion(destId, dest, column.name, scalar, selected.index)
        } yield (scalar, t)
    }

  private def validateCoercion(
      destId: D,
      dest: Destination[F],
      column: String,
      scalar: ColumnType.Scalar,
      selected: TypeIndex)
      : Either[ResultPushError[D], Unit] = {

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
      : Either[ResultPushError[D], dest.Type] = {

    import dest._
    import ParamType._

    def checkBounds(b: Int Ior Int, i: Int): SBoolean =
      b.fold(_ <= i, _ >= i, (min, max) => min <= i && i <= max)

    def validatedParam[A](label: String, formal: Formal[A], actual: ∃[Actual])
        : ValidatedNel[ParamError, A] =
      (formal, actual) match {
        case (Boolean(_), ∃(Boolean(Const(b)))) =>
          b.validNel

        case (Integer(Integer.Args(None, None, _)), ∃(Integer(Const(i)))) =>
          i.validNel

        case (Integer(Integer.Args(Some(bounds), None, _)), ∃(Integer(Const(i)))) =>
          if (checkBounds(bounds, i))
            i.validNel
          else
            ParamError.IntOutOfBounds(label, i, bounds).invalidNel

        case (Integer(Integer.Args(None, Some(step), _)), ∃(Integer(Const(i)))) =>
          if (step(i))
            i.validNel
          else
            ParamError.IntOutOfStep(label, i, step).invalidNel

        case (Integer(Integer.Args(Some(bounds), Some(step), _)), ∃(Integer(Const(i)))) =>
          if (!checkBounds(bounds, i))
            ParamError.IntOutOfBounds(label, i, bounds).invalidNel
          else if (!step(i))
            ParamError.IntOutOfStep(label, i, step).invalidNel
          else
            i.validNel

        case (Enum(possiblities), ∃(EnumSelect(Const(key)))) =>
          possiblities.lookup(key)
            .toValidNel(ParamError.ValueNotInEnum(label, key, possiblities.keys))

        case _ => ParamError.ParamMismatch(label, ∃(formal), actual).invalidNel
      }

    typeIdOrdinal.getOption(selected.index.ordinal) match {
      case Some(id) => construct(id) match {
        case Left(t) => Right(t)

        case Right(ctor) =>
          val back = ctor match {
            case Constructor.Unary(Labeled(l1, p1), f) =>
              selected.args match {
                case List(a1) =>
                  validatedParam(l1, p1, a1).map(f)

                case Nil =>
                  ParamError.ParamMissing(l1, p1).invalidNel

                case List(_, r, rs @ _*) =>
                  ParamError.ExcessiveParams(1, 2 + rs.length, NonEmptyList.of(r, rs: _*)).invalidNel
              }

            case Constructor.Binary(Labeled(l1, p1), Labeled(l2, p2), f) =>
              selected.args match {
                case List(a1, a2) =>
                  (validatedParam(l1, p1, a1), validatedParam(l2, p2, a2)).mapN(f)

                case List(a1) =>
                  ParamError.ParamMissing(l2, p2).invalidNel

                case Nil =>
                  NonEmptyList.of(
                    ParamError.ParamMissing(l1, p1),
                    ParamError.ParamMissing(l2, p2))
                    .invalid

                case List(_, _, r, rs @ _*) =>
                  ParamError.ExcessiveParams(2, 3 + rs.length, NonEmptyList.of(r, rs: _*)).invalidNel
              }

            case Constructor.Ternary(Labeled(l1, p1), Labeled(l2, p2), Labeled(l3, p3), f) =>
              selected.args match {
                case List(a1, a2, a3) =>
                  (validatedParam(l1, p1, a1), validatedParam(l2, p2, a2), validatedParam(l3, p3, a3)).mapN(f)

                case List(_, _) =>
                  ParamError.ParamMissing(l3, p3).invalidNel

                case List(_) =>
                  NonEmptyList.of(
                    ParamError.ParamMissing(l2, p2),
                    ParamError.ParamMissing(l3, p3))
                    .invalid

                case Nil =>
                  NonEmptyList.of(
                    ParamError.ParamMissing(l1, p1),
                    ParamError.ParamMissing(l2, p2),
                    ParamError.ParamMissing(l3, p3))
                    .invalid

                case List(_, _, _, r, rs @ _*) =>
                  ParamError.ExcessiveParams(3, 4 + rs.length, NonEmptyList.of(r, rs: _*)).invalidNel
              }
          }

          back.leftMap(TypeConstructionFailed(destId, name, id.label, _)).toEither
      }

      case None =>
        Left(TypeNotFound(destId, name, selected.index))
    }
  }

  private def destination[E >: DestinationNotFound[D] <: ResultPushError[D]](
      destinationId: D)
      : EitherT[F, E, Destination[F]] =
    OptionT(lookupDestination(destinationId))
      .toRight[E](DestinationNotFound(destinationId))

  private def lowerKey(k: D :: Option[ResourcePath] :: HNil): D :: ResourcePath :: HNil =
    k.updateWith((_: Option[ResourcePath]).get)
}

private[impl] object DefaultResultPush {
  private sealed trait ActiveState[F[_], Q] extends Product with Serializable {
    def at: Instant
    def limit: Option[Long]
    def terminal: Deferred[F, Status.Terminal]
  }

  private object ActiveState {
    final case class StartAccepted[F[_], Q](
        config: ∃[PushConfig[?, Q]],
        at: Instant,
        limit: Option[Long],
        terminal: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q]

    final case class UpdateAccepted[F[_], Q](
        push: ∃[Push[?, Q]],
        at: Instant,
        terminal: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q] {

      val limit = None
    }

    final case class PushRunning[F[_], Q](
        push: ∃[Push[?, Q]],
        at: Instant,
        acceptedAt: Instant,
        limit: Option[Long],
        terminal: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q]
  }

  def apply[F[_]: Concurrent: Timer, D: Codec: Order: Show, Q, R](
      maxConcurrentPushes: Int,
      maxOutstandingPushes: Int,
      lookupDestination: D => F[Option[Destination[F]]],
      evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), R],
      render: ResultRender[F, R],
      pushes: PrefixStore.SCodec[F, D :: ResourcePath :: HNil, ∃[Push[?, Q]]],
      offsets: Store[F, D :: ResourcePath :: HNil, ∃[OffsetKey.Actual]])
      : Resource[F, ResultPush[F, D, Q]] = {

    def acceptedAsOf(acceptedAt: Instant, startedAt: JobStarted): SBoolean =
      acceptedAt.compareTo(epochToInstant(startedAt.epoch)) <= 0

    def handleEvent(
        active: ConcurrentMap[D :: Option[ResourcePath] :: HNil, ActiveState[F, Q]],
        log: Logger[F])
        : JobEvent[D :: ResourcePath :: HNil] => F[Unit] = {

      def complete[A](
          running: ActiveState.PushRunning[F, Q],
          id: D :: ResourcePath :: HNil,
          push: Push[A, Q],
          terminal: Status.Terminal,
          limit: Option[Long],
          deferred: Deferred[F, Status.Terminal]): F[Unit] = {

        val terminate = for {
          _ <- Sync[F].delay(active.remove(liftKey(id), running))
          _ <- deferred.complete(terminal)
        } yield ()

        pushes
          .insert(id, ∃[Push[?, Q]](push.copy(status = terminal)))
          .guarantee(terminate.uncancelable)
      }

      {
        case jev @ JobEvent.Completed(id, start, duration) =>
          Sync[F].delay(active.get(liftKey(id))) flatMap {
            case s @ ActiveState.PushRunning(p, _, acceptedAt, limit, deferred)
                if acceptedAsOf(acceptedAt, start) =>

              val finished = Status.Finished(
                epochToInstant(start.epoch),
                epochToInstant(start.epoch + duration),
                limit)

              complete(s, id, p.value, finished, limit, deferred)
                .productR(log.debug(debugTerminal("finished", id, finished)))

            case other =>
              log.debug(debugIgnored(id, jev, Option(other)))
          }

        case jev @ JobEvent.Failed(id, start, duration, ex) =>
          Sync[F].delay(active.get(liftKey(id))) flatMap {
            case s @ ActiveState.PushRunning(p, _, acceptedAt, limit, deferred)
                if acceptedAsOf(acceptedAt, start) =>

              val failed = Status.Failed(
                epochToInstant(start.epoch),
                epochToInstant(start.epoch + duration),
                limit,
                ex.toString)

              complete(s, id, p.value, failed, limit, deferred)
                .productR(log.error(ex)(debugTerminal("failed", id, failed)))

            case other =>
              log.debug(debugIgnored(id, jev, Option(other)))
          }

        // FIXME: case JobEvent.Canceled
      }
    }

    // A custom comparator with the property that, forall d: D, p: ResourcePath,
    // d :: Some(p) :: HNil < d :: None :: HNil.
    //
    // This gives us a means of selecting all paths for a destination via
    // range queries where the lower bound is d :: Some(/) :: HNil and the upper
    // bound is d :: None :: HNil.
    def prefixComparator: Comparator[D :: Option[ResourcePath] :: HNil] =
      new Comparator[D :: Option[ResourcePath] :: HNil] {
        def compare(
            x: D :: Option[ResourcePath] :: HNil,
            y: D :: Option[ResourcePath] :: HNil)
            : Int = {

          val d = x.head.compare(y.head)

          if (d == 0)
            (x.select[Option[ResourcePath]], y.select[Option[ResourcePath]]) match {
              case (None, None) => 0
              case (None, _) => 1
              case (_, None) => -1
              case (Some(xp), Some(yp)) => xp.compare(yp)
            }
          else
            d
        }
      }

    def acquire(jobManager: JobManager[F, D :: ResourcePath :: HNil, Nothing])
        : Resource[F, ResultPush[F, D, Q]] =
      for {
        active <-
          Resource liftF {
            Sync[F].delay(new ConcurrentSkipListMap[D :: Option[ResourcePath] :: HNil, ActiveState[F, Q]](
              prefixComparator))
          }

        log <- Resource.liftF(Slf4jLogger.create[F])

        _ <- Concurrent[F].background(jobManager.events.evalMap(handleEvent(active, log)).compile.drain)

        maker = Sync[F] delay {
          new DefaultResultPush(
            lookupDestination,
            evaluator,
            jobManager,
            render,
            active,
            pushes,
            offsets,
            log)
        }

        back <- Resource.make(maker)(_.cancelAll)
      } yield back

    val jm =
      JobManager[F, D :: ResourcePath :: HNil, Nothing](
        jobConcurrency = maxConcurrentPushes,
        jobLimit = maxOutstandingPushes,
        eventsLimit = maxOutstandingPushes)

    jm.flatMap(acquire)
  }


  ////

  private def liftKey[D, T](key: D :: T :: HNil): D :: Option[T] :: HNil =
    key.updateWith(Option(_: T))

  private def epochToInstant(e: FiniteDuration): Instant =
    Instant.ofEpochMilli(e.toMillis)

  private def debugActiveState[F[_]](st: ActiveState[F, _]): String =
    st match {
      case ActiveState.StartAccepted(_, at, _, _) =>
        s"StartAccepted[$at]"

      case ActiveState.UpdateAccepted(_, at, _) =>
        s"UpdateAccepted[$at]"

      case ActiveState.PushRunning(_, st, ac, _, _) =>
        s"PushRunning[started=$st, accepted=$ac]"
    }

  private val debugJobEvent: JobEvent[_] => String = {
    case JobEvent.Completed(_, start, dur) =>
      val st = epochToInstant(start.epoch)
      val ed = epochToInstant(start.epoch + dur)
      s"Completed[start=$st, end=$ed]"

    case JobEvent.Failed(_, start, dur, err) =>
      val st = epochToInstant(start.epoch)
      val ed = epochToInstant(start.epoch + dur)
      s"Failed[start=$st, end=$ed, err=$err]"

    case JobEvent.Canceled(_, start, dur) =>
      val st = epochToInstant(start.epoch)
      val ed = epochToInstant(start.epoch + dur)
      s"Canceled[start=$st, end=$ed]"
  }

  private def debugKey[D: Show](k: D :: ResourcePath :: HNil): String =
    s"[${k.select[D].show}:${Path.posixCodec.printPath(k.select[ResourcePath].toPath)}]"

  private def debugTerminal[D: Show](
      desc: String,
      id: D :: ResourcePath :: HNil,
      status: Status.Terminal)
      : String =
    s"${debugKey(id)} Push $desc (elapsed ${Status.elapsed(status)})"

  private def debugIgnored[F[_], D: Show](
      id: D :: ResourcePath :: HNil,
      event: JobEvent[D :: ResourcePath :: HNil],
      astate: Option[ActiveState[F, _]])
      : String = {
    val stateStr = astate.fold("None")(debugActiveState)
    s"${debugKey(id)} Ignored push termination event: event=${debugJobEvent(event)}, activeState=$stateStr"
  }
}
