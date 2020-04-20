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

package quasar.impl.intentions

import slamdata.Predef._

import quasar.Condition
import quasar.api.intentions._, IntentionError._
import quasar.connector.scheduler.Scheduler

import cats.Monad
import cats.data.{OptionT, EitherT}
import cats.implicits._

import fs2.Stream

private [impl] final class DefaultIntentions[
    F[_]: Monad,
    I, II, C] private (
    schedulerIds: Stream[F, I],
    scheduler: I => F[Option[Scheduler[F, II, C]]])
    extends Intentions[F, I, II, C] {

  def allIntentions: Stream[F, (I, II, C)] = for {
    schedulerId <- schedulerIds
    eStream <- Stream.eval(schedulerIntentions(schedulerId))
    stream <- eStream match {
      case Left(_) => Stream.empty
      case Right(s) => Stream.emit(s)
    }
    (intentionId, config) <- stream
  } yield (schedulerId, intentionId, config)

  def schedulerIntentions(schedulerId: I): F[Either[SchedulerNotFound[I], Stream[F, (II, C)]]] =
    lookupScheduler[SchedulerNotFound[I]](schedulerId)
      .map(_.entries)
      .value

  def add(schedulerId: I, config: C): F[Either[IntentionError[I, II, C], II]] =
    lookupScheduler[IntentionError[I, II, C]](schedulerId)
      .flatMap(s => EitherT(s.addIntention(config)).leftMap(x => x: IntentionError[I, II, C]))
      .value

  def lookup(schedulerId: I, intentionId: II): F[Either[IntentionError[I, II, C], C]] =
    lookupScheduler[IntentionError[I, II, C]](schedulerId)
      .flatMap(s => EitherT(s.lookupIntention(intentionId)).leftMap(x => x: IntentionError[I, II, C]))
      .value

  def edit(schedulerId: I, intentionId: II, config: C): F[Condition[IntentionError[I, II, C]]] =
    lookupScheduler[IntentionError[I, II, C]](schedulerId)
      .flatMap(s => wrapCondition[SchedulingError[II, C]](s.editIntention(intentionId, config)))
      .value
      .map(Condition.eitherIso(_))

  def delete(schedulerId: I, intentionId: II): F[Condition[IntentionError[I, II, C]]] =
    lookupScheduler[IntentionError[I, II, C]](schedulerId)
      .flatMap(s => wrapCondition[IntentionNotFound[II]](s.deleteIntention(intentionId)))
      .value
      .map(Condition.eitherIso(_))

  private def lookupScheduler[E >: SchedulerNotFound[I] <: IntentionError[I, II, C]](i: I): EitherT[F, E, Scheduler[F, II, C]] =
    OptionT(scheduler(i)).toRight(SchedulerNotFound(i): E)

  private def wrapCondition[E <: IntentionError[I, II, C]](act: F[Condition[E]]): EitherT[F, IntentionError[I, II, C], Unit] = {
    val e: F[Either[IntentionError[I, II, C], Unit]] = act map (Condition.eitherIso.get(_))
    EitherT(e)
  }
}

object DefaultIntentions {
  private[impl] def apply[
      F[_]: Monad,
      I, II, C](
      schedulerIds: Stream[F, I],
      scheduler: I => F[Option[Scheduler[F, II, C]]])
      : Intentions[F, I, II, C] =
    new DefaultIntentions(schedulerIds, scheduler)
}
