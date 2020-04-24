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
import quasar.connector.Offset
import quasar.connector.destination._
import quasar.connector.render.ResultRender
import quasar.impl.storage.{IndexedStore, PrefixStore}

import java.time.Instant
import java.util.Comparator
import java.util.concurrent.{ConcurrentMap, ConcurrentNavigableMap, ConcurrentSkipListMap}

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, MILLISECONDS}

import cats.{Applicative, Order}
import cats.data.{Const, EitherT, Ior, OptionT, NonEmptyList}
import cats.effect.{Concurrent, Resource, Sync, Timer}
import cats.effect.concurrent.Deferred
import cats.effect.implicits._
import cats.implicits._

import fs2.Stream
import fs2.job.{JobManager, Job, Event => JobEvent, Timestamp => JobStarted}

import shapeless._
import shapeless.HList.ListCompat._

import shims.orderToCats

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
// 1. StartAccepted(config, startedAt, limit), nothing persisted yet, but status reflects
//    acceptance of new config
// 2. When the run action is actually scheduled, if the current status is start accepted,
//    a new push is persisted with the status set to `Unknown`
// 3. If the server crashes or whatnot, the persisted state will actually reflect we
//    have no idea what state the push is in
// 4. When the push completes successfully, persist it with the terminal status
//
//

// FIXME: Make a pass through `start` with an eye towards cancellation/failure and ensuring
//        state is correct in their presence.

// TODO[logging]: Need to log when a push terminates
// TODO[clustering]: Need to decide how we'll ensure a job is only ever running on a single node.
final class DefaultResultPush[
    F[_]: Concurrent: Timer, D <: AnyRef, Q, R] private (
    lookupDestination: D => F[Option[Destination[F]]],
    evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), Stream[F, R]],
    jobManager: JobManager[F, D :: ResourcePath :: HNil, Nothing],
    render: ResultRender[F, R],
    active: ConcurrentNavigableMap[D :: Option[ResourcePath] :: HNil, DefaultResultPush.ActiveState[F, Q]],
    pushes: PrefixStore[F, D :: ResourcePath :: HNil, ∃[Push[?, Q]]],
    offsets: IndexedStore[F, D :: ResourcePath :: HNil, ∃[OffsetKey.Actual]])
    extends ResultPush[F, D, Q] {

  import DefaultResultPush.{ActiveState, liftKey}
  import ResultPushError._

  def cancel(destinationId: D, path: ResourcePath): F[Condition[DestinationNotFound[D]]] =
    ???
/*
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
*/

  def cancelAll: F[Unit] =
    ???
    //jobManager.jobIds.flatMap(_.traverse_(jobManager.cancel(_)))

  def coerce(destinationId: D, scalar: ColumnType.Scalar)
      : F[Either[DestinationNotFound[D], TypeCoercion[CoercedType]]] =
    destination[DestinationNotFound[D]](destinationId)
      .map(coerceWith(_, scalar))
      .value

  def destinationStatus(destinationId: D)
      : F[Either[DestinationNotFound[D], Map[ResourcePath, ∃[Push[?, Q]]]]] = {

    def overrideWithActive(m: Map[ResourcePath, ∃[Push[?, Q]]]): F[Map[ResourcePath, ∃[Push[?, Q]]]] =
      Sync[F] delay {
        val activeForDest =
          active
            .subMap(
              destinationId :: Some(ResourcePath.root()) :: HNil,
              destinationId :: None :: HNil)
            .entrySet.iterator.asScala
            .map(e => e.getKey -> e.getValue)
            .collect { case (_ #: Some(path) #: HNil, v) => (path, v) }

        activeForDest.foldLeft(m) {
          case (m, (path, ActiveState.StartAccepted(cfg, ts, lim, _))) =>
            m.updated(path, ∃[Push[?, Q]](Push(cfg.value, ts, Status.Accepted(ts, lim))))

          case (m, (path, ActiveState.UpdateAccepted(p, ts, _))) =>
            m.updated(path, ∃[Push[?, Q]](p.value.copy(status = Status.Accepted(ts, None))))

          case (m, (path, ActiveState.PushRunning(p, ts, _, lim, _))) =>
            m.updated(path, ∃[Push[?, Q]](p.value.copy(status = Status.Running(ts, lim))))
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

    type Errs = NonEmptyList[ResultPushError[D]]

    def err(rpe: ResultPushError[D]): Errs = NonEmptyList.one(rpe)

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
            createSink(dest).toRight[ResultPushError[D]](FullNotSupported(destinationId))
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

    val started = for {
      dest <- destination(destinationId).leftMap(err)

      cfg = config.value

      key = destinationId :: cfg.path :: HNil

      _ <- EitherT(Sync[F].delay(active.containsKey(liftKey(key))) map {
        case true => Left(err(PushAlreadyRunning(destinationId, cfg.path)))
        case false => Right(())
      })

      typedOutputColumns <- EitherT.fromEither[F] {
        cfg.columns
          .traverse(typedColumn(destinationId, dest, _).toValidatedNel)
          .toEither
      }

      sinked <- cfg match {
        case PushConfig.Full(path, q, _) =>
          handleFull(dest)(path, q, typedOutputColumns)

        case PushConfig.Incremental(path, q, _, resumeCfg, initOffset) =>
          handleIncremental(dest)(path, q, typedOutputColumns, resumeCfg, initOffset)
            .map(_.evalMap(o => offsets.insert(key, ∃(o))))
      }

      terminal <- EitherT.right[Errs](Deferred[F, Status.Terminal])
      acceptedAt <- EitherT.right[Errs](instantNow)
      accepted = ActiveState.StartAccepted(config, acceptedAt, limit, terminal)

      alreadyRunning <- EitherT.right[Errs](Sync[F] delay {
        Option(active.putIfAbsent(liftKey(key), accepted)).isDefined
      })

      _ <- if (alreadyRunning)
        EitherT.leftT[F, Unit](err(PushAlreadyRunning(destinationId, cfg.path)))
      else
        EitherT.rightT[F, Errs](())

      preamble = for {
        runningAt <- instantNow

        unknownPush = ∃[Push[?, Q]](Push(cfg, acceptedAt, Status.Unknown(runningAt, limit)))
        running = ActiveState.PushRunning(unknownPush, runningAt, acceptedAt, limit, terminal)

        _ <- Sync[F].delay(active.replace(liftKey(key), accepted, running))
        _ <- pushes.insert(key, unknownPush)
        _ <- offsets.delete(key)
      } yield ()

      job = Stream.eval_(preamble) ++ sinked

      submitted <- EitherT.right[Errs](jobManager.submit(Job(key, job.map(Right(_)))))

      _ <- if (submitted)
        EitherT.rightT[F, Errs](())
      else
        // TODO[logging]: this shouldn't happen, so should log if it ever does
        EitherT.left[Unit](for {
          _ <- Sync[F].delay(active.remove(liftKey(key), accepted))
          canceledAt <- instantNow
          _ <- terminal.complete(Status.Canceled(acceptedAt, canceledAt, limit))
        } yield err(PushAlreadyRunning(destinationId, cfg.path)))

    } yield terminal.get

    started.value
  }

  def update(destinationId: D, path: ResourcePath)
      : F[Either[NonEmptyList[ResultPushError[D]], F[Status.Terminal]]] =
    ???


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

  private def typedColumn(destId: D, dest: Destination[F], column: PushConfig.OutputColumn)
      : Either[ResultPushError[D], Column[(ColumnType.Scalar, dest.Type)]] =
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

  private def destination[E >: DestinationNotFound[D] <: ResultPushError[D]](
      destinationId: D)
      : EitherT[F, E, Destination[F]] =
    OptionT(lookupDestination(destinationId))
      .toRight[E](DestinationNotFound(destinationId))
}

object DefaultResultPush {
  private sealed trait ActiveState[F[_], Q] extends Product with Serializable

  private object ActiveState {
    final case class StartAccepted[F[_], Q](
        config: ∃[PushConfig[?, Q]],
        at: Instant,
        limit: Option[Long],
        deferred: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q]

    final case class UpdateAccepted[F[_], Q](
        push: ∃[Push[?, Q]],
        at: Instant,
        deferred: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q]

    final case class PushRunning[F[_], Q](
        push: ∃[Push[?, Q]],
        at: Instant,
        acceptedAt: Instant,
        limit: Option[Long],
        deferred: Deferred[F, Status.Terminal])
        extends ActiveState[F, Q]
  }

  def apply[F[_]: Concurrent: Timer, D <: AnyRef: Order, Q, R](
      maxConcurrentPushes: Int,
      lookupDestination: D => F[Option[Destination[F]]],
      evaluator: QueryEvaluator[Resource[F, ?], (Q, Option[Offset]), Stream[F, R]],
      render: ResultRender[F, R],
      pushes: PrefixStore[F, D :: ResourcePath :: HNil, ∃[Push[?, Q]]],
      offsets: IndexedStore[F, D :: ResourcePath :: HNil, ∃[OffsetKey.Actual]])
      : Resource[F, DefaultResultPush[F, D, Q, R]] = {

    def epochToInstant(e: FiniteDuration): Instant =
      Instant.ofEpochMilli(e.toMillis)

    def acceptedAsOf(acceptedAt: Instant, startedAt: JobStarted): SBoolean =
      acceptedAt.compareTo(epochToInstant(startedAt.epoch)) <= 0

    def handleEvent(active: ConcurrentMap[D :: Option[ResourcePath] :: HNil, ActiveState[F, Q]])
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
        case JobEvent.Completed(id, start, duration) =>
          Sync[F].delay(active.get(liftKey(id))) flatMap {
            case s @ ActiveState.PushRunning(p, _, acceptedAt, limit, deferred)
                if acceptedAsOf(acceptedAt, start) =>

              val finished = Status.Finished(
                epochToInstant(start.epoch),
                epochToInstant(start.epoch + duration),
                limit)

              complete(s, id, p.value, finished, limit, deferred)

            case _ => Applicative[F].unit
          }

        case JobEvent.Failed(id, start, duration, ex) =>
          Sync[F].delay(active.get(liftKey(id))) flatMap {
            case s @ ActiveState.PushRunning(p, _, acceptedAt, limit, deferred)
                if acceptedAsOf(acceptedAt, start) =>

              val failed = Status.Failed(
                epochToInstant(start.epoch),
                epochToInstant(start.epoch + duration),
                limit,
                ex.toString)

              complete(s, id, p.value, failed, limit, deferred)

            case _ => Applicative[F].unit
          }
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
        : F[DefaultResultPush[F, D, Q, R]] =
      for {
        active <-
          Sync[F].delay(new ConcurrentSkipListMap[D :: Option[ResourcePath] :: HNil, ActiveState[F, Q]](
            prefixComparator))

        _ <- Concurrent[F].start(jobManager.events.evalMap(handleEvent(active)).compile.drain)
      } yield {
        new DefaultResultPush(
          lookupDestination,
          evaluator,
          jobManager,
          render,
          active,
          pushes,
          offsets)
      }

    val jm =
      JobManager[F, D :: ResourcePath :: HNil, Nothing](
        jobLimit = maxConcurrentPushes,
        eventsLimit = maxConcurrentPushes)

    jm flatMap { jobManager =>
      Resource.make(acquire(jobManager))(_.cancelAll)
    }
  }

  private def liftKey[D, T](key: D :: T :: HNil): D :: Option[T] :: HNil =
    key.updateWith(Option(_: T))
}
