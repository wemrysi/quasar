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

package quasar.impl.scheduler

import slamdata.Predef._

import quasar.api.scheduler._
import quasar.api.scheduler.SchedulerError._
import quasar.connector.MonadResourceErr
import quasar.connector.scheduler.{Scheduler, SchedulerBuilder}
import quasar.impl.IncompatibleModuleException.linkScheduler

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.data.{EitherT, OptionT}
import cats.effect._
import cats.effect.concurrent.Ref
import cats.implicits._
import cats.MonadError

trait SchedulerBuilders[F[_], I, C, CC] {
  def create(ref: SchedulerRef[C]): EitherT[Resource[F, ?], CreateError[C], Scheduler[F, I, CC]]
  def sanitizeRef(inp: SchedulerRef[C]): F[SchedulerRef[C]]
  def supportedTypes: F[Set[SchedulerType]]
  def enable(module: SchedulerBuilder[F]): F[Unit]
  def disable(mt: SchedulerType): F[Unit]
}

object SchedulerBuilders {
  private[impl] def apply[F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr](
      builders: List[SchedulerBuilder[F]])
      : F[SchedulerBuilders[F, Array[Byte], Json, Json]] = {

    val builderMap: Map[SchedulerType, SchedulerBuilder[F]] =
      Map(builders.map(ss => (ss.schedulerType, ss)):_*)

    Ref.of[F, Map[SchedulerType, SchedulerBuilder[F]]](builderMap) map { (ref: Ref[F, Map[SchedulerType, SchedulerBuilder[F]]]) =>
      def getBuilder(i: SchedulerType): F[Option[SchedulerBuilder[F]]] = ref.get map (_.get(i))

      new SchedulerBuilders[F, Array[Byte], Json, Json] {
        def create(ref: SchedulerRef[Json])
            : EitherT[Resource[F, ?], CreateError[Json], Scheduler[F, Array[Byte], Json]] =
          for {
            tys <- EitherT.right(Resource.liftF(supportedTypes))
            builder <- OptionT(Resource.liftF(getBuilder(ref.kind))).toRight(SchedulerUnsupported(ref.kind, tys))
            res <- handleInitErrors(ref.kind, builder.scheduler(ref.config))
          } yield res

        def sanitizeRef(inp: SchedulerRef[Json]): F[SchedulerRef[Json]] = getBuilder(inp.kind) map {
          case None => inp.copy(config = jEmptyObject)
          case Some(x) => inp.copy(config = x.sanitizeConfig(inp.config))
        }
        def supportedTypes: F[Set[SchedulerType]] =
          ref.get map (_.keySet)

        def enable(m: SchedulerBuilder[F]): F[Unit] = ref.update(_.updated(m.schedulerType, m))
        def disable(mt: SchedulerType): F[Unit] = ref.update(_ - mt)
      }
    }
  }

  private def handleInitErrors[F[_]: MonadError[?[_], Throwable], A](
      kind: SchedulerType,
      res: => Resource[F, Either[InitializationError[Json], A]])
      : EitherT[Resource[F, ?], CreateError[Json], A] = {
    EitherT(linkScheduler(kind, res)).leftMap(ie => ie: CreateError[Json])
  }
}
