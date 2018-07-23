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

package quasar.impl.table

import slamdata.Predef._

import quasar.Condition
import quasar.api.QueryEvaluator

import cats.effect.{ConcurrentEffect, Effect}

import fs2.{async, Stream}
import fs2.async.mutable.Queue

// monad/traverse syntax conflict
import scalaz.{Equal, OptionT, Scalaz}, Scalaz._

import shims._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import java.time.OffsetDateTime
import java.util.concurrent.ConcurrentHashMap

class PreparationsManager[F[_]: Effect, I, Q, R] private (
    evaluator: QueryEvaluator[F, Q, R],
    notificationsQ: Queue[F, Option[PreparationsManager.TableEvent[I]]],
    background: Stream[F, Nothing] => F[Unit])(
    runToStore: (I, R) => F[Stream[F, Unit]])(
    implicit ec: ExecutionContext) {

  import PreparationsManager._

  private val pending: ConcurrentHashMap[I, F[Unit]] =
    new ConcurrentHashMap[I, F[Unit]]

  private val ongoing: ConcurrentHashMap[I, Preparation[F]] =
    new ConcurrentHashMap[I, Preparation[F]]

  val F = Effect[F]

  val notifications: Stream[F, TableEvent[I]] = notificationsQ.dequeue.unNoneTerminate

  // fake Equal for fake parametricity
  def prepareTable(tableId: I, query: Q)(implicit I: Equal[I]): F[Condition[InProgressError[I]]] = {
    for {
      s <- async.signalOf[F, Boolean](false)
      cancel = s.set(true)

      pcheck <- F.delay(Option(pending.putIfAbsent(tableId, cancel)).isDefined)

      back <- if (pcheck) {
        Condition.abnormal(InProgressError(tableId)).point[F]
      } else {
        F.delay(ongoing.contains(tableId)) flatMap { ocheck =>
          if (ocheck) {
            F.delay(pending.remove(tableId)).as(Condition.abnormal(InProgressError(tableId)))
          } else {
            for {
              result <- evaluator.evaluate(query)
              persist <- runToStore(tableId, result)

              configured = Stream.eval(F.delay(OffsetDateTime.now())) flatMap { start =>
                val preparation = Preparation(cancel, start)
                val halted = persist.interruptWhen(s)

                val handled = halted handleErrorWith { t =>
                  val eff = for {
                    end <- F.delay(OffsetDateTime.now())
                    _ <- s.set(true)    // prevent the onComplete handler

                    _ <- notificationsQ.enqueue1(
                      Some(
                        TableEvent.PreparationErrored(
                          tableId,
                          start,
                          (end.toEpochSecond - start.toEpochSecond).millis,
                          t)))
                  } yield ()

                  Stream.eval_(eff)
                } onComplete {
                  val eff = for {
                    canceled <- s.get

                    _ <- if (canceled) {
                      ().point[F]
                    } else {
                      for {
                        end <- F.delay(OffsetDateTime.now())

                        _ <- notificationsQ.enqueue1(
                          Some(
                            TableEvent.PreparationSucceeded(
                              tableId,
                              start,
                              (end.toEpochSecond - start.toEpochSecond).millis)))
                      } yield ()
                    }
                  } yield ()

                  Stream.eval_(eff)
                }

                Stream.bracket(
                  initiateState(tableId, preparation))(
                  flag => if (flag) handled else Stream.empty,
                  _ => F.delay(ongoing.remove(tableId, preparation)).void)
              }

              _ <- background(configured.drain)
            } yield Condition.normal[InProgressError[I]]()
          }
        }
      }
    } yield back
  }

  private def initiateState(tableId: I, preparation: Preparation[F]): F[Boolean] = {
    for {
     // I don't think this can ever fail?
      successOpt <- F.delay(Option(ongoing.putIfAbsent(tableId, preparation)))
      _ <- successOpt.traverse(p => F.delay(Option(pending.remove(tableId, p.cancel))))
    } yield !successOpt.isDefined
  }

  def preparationStatus(tableId: I)(implicit I: Equal[I]): F[Status] = {
    for {
      ongoingStatus <- F.delay(Option(ongoing.get(tableId)).map(_.start))
      pendingStatus <- F.delay(Option(pending.get(tableId)))
    } yield {
      ongoingStatus.map(Status.Started)
        .orElse(pendingStatus.as(Status.Pending))
        .getOrElse(Status.Unknown)
    }
  }

  def cancelPreparation(tableId: I)(implicit I: Equal[I]): F[Condition[NotInProgressError[I]]] = {
    val removePending = for {
      cancel <- OptionT(F.delay(Option(pending.get(tableId))))
      removed <- F.delay(pending.remove(tableId, cancel)).liftM[OptionT]

      _ <- if (removed)
        cancel.liftM[OptionT]
      else
        ().point[OptionT[F, ?]]
    } yield ()

    val removeOngoing = for {
      live <- OptionT(F.delay(Option(ongoing.get(tableId))))
      removed <- F.delay(ongoing.remove(tableId, live)).liftM[OptionT]

      _ <- if (removed)
        live.cancel.liftM[OptionT]
      else
        ().point[OptionT[F, ?]]
    } yield ()

    (removePending.orElse(removeOngoing)).run map {
      case Some(_) => Condition.Normal()
      case None => Condition.Abnormal(NotInProgressError(tableId))
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  def cancelAll(implicit I: Equal[I]): F[Unit] = {
    for {
      pendingKeys <- F.delay(pending.keys.asScala.toList)
      ongoingKeys <- F.delay(ongoing.keys.asScala.toList)

      keys = (pendingKeys ++ ongoingKeys).distinct

      results <- keys.traverse(cancelPreparation)

      _ <- if (results.isEmpty)
        ().point[F]
      else
        cancelAll   // handle race condition of new enqueue
    } yield ()
  }
}

object PreparationsManager {

  @SuppressWarnings(Array("org.wartremover.warts.DefaultArguments"))
  def apply[F[_]: ConcurrentEffect, I, Q, R](
      evaluator: QueryEvaluator[F, Q, R],
      maxStreams: Int = 10,
      maxNotifications: Int = 10)(
      runToStore: (I, R) => F[Stream[F, Unit]])(
      implicit ec: ExecutionContext): Stream[F, PreparationsManager[F, I, Q, R]] = {

    for {
      q <-
        Stream.eval(async.boundedQueue[F, Stream[F, Nothing]](maxStreams))

      notificationsQ <-
        Stream.eval(async.boundedQueue[F, Option[TableEvent[I]]](maxNotifications))

      emit = Stream(new PreparationsManager[F, I, Q, R](evaluator, notificationsQ, q.enqueue1(_))(runToStore))

      // we have to be explicit here because scalaz's MonadSyntax includes .join
      back <- emit.concurrently(Stream.InvariantOps(q.dequeue).join(maxStreams)) onComplete {
        Stream.eval_(notificationsQ.enqueue1(None))
      }
    } yield back
  }

  private final case class Preparation[F[_]](
      cancel: F[Unit],
      start: OffsetDateTime)

  final case class InProgressError[I](tableId: I)
  final case class NotInProgressError[I](tableId: I)

  sealed trait Status extends Product with Serializable

  object Status {
    final case class Started(start: OffsetDateTime) extends Status
    case object Pending extends Status
    case object Unknown extends Status
  }

  sealed trait TableEvent[I] extends Product with Serializable

  object TableEvent {
    final case class PreparationErrored[I](
        tableId: I,
        start: OffsetDateTime,
        duration: Duration,
        t: Throwable) extends TableEvent[I]

    final case class PreparationSucceeded[I](
        tableId: I,
        start: OffsetDateTime,
        duration: Duration) extends TableEvent[I]
  }
}
