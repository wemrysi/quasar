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
import quasar.connector.scheduler.{Scheduler, SchedulerModule}
import quasar.impl.IncompatibleModuleException.linkScheduler

import argonaut.Json
import argonaut.Argonaut.jEmptyObject

import cats.data.EitherT
import cats.effect._
import cats.implicits._
import cats.MonadError

trait SchedulerModules[F[_], I, C, CC] {
  def create(ref: SchedulerRef[C]): EitherT[Resource[F, ?], CreateError[C], Scheduler[F, I, CC]]
  def sanitizeRef(inp: SchedulerRef[C]): SchedulerRef[C]
  def supportedTypes: F[Set[SchedulerType]]
}

object SchedulerModules {
  private[impl] def apply[F[_]: ConcurrentEffect: ContextShift: Timer: MonadResourceErr](
      modules: List[SchedulerModule])
      : SchedulerModules[F, Array[Byte], Json, Json] = {
    lazy val moduleSet: Set[SchedulerType] =
      Set(modules.map(_.schedulerType):_*)
    lazy val moduleMap: Map[SchedulerType, SchedulerModule] =
      Map(modules.map(ss => (ss.schedulerType, ss)):_*)

    new SchedulerModules[F, Array[Byte], Json, Json] {
      def create(ref: SchedulerRef[Json])
          : EitherT[Resource[F, ?], CreateError[Json], Scheduler[F, Array[Byte], Json]] =
        moduleMap.get(ref.kind) match {
          case None =>
            EitherT.leftT(SchedulerUnsupported(ref.kind, moduleSet): CreateError[Json])
          case Some(module) =>
            handleInitErrors(ref.kind, module.scheduler[F](ref.config))
        }
      def sanitizeRef(inp: SchedulerRef[Json]): SchedulerRef[Json] = moduleMap.get(inp.kind) match {
        case None => inp.copy(config = jEmptyObject)
        case Some(x) => inp.copy(config = x.sanitizeConfig(inp.config))
      }
      def supportedTypes: F[Set[SchedulerType]] =
        moduleSet.pure[F]
    }
  }

  private def handleInitErrors[F[_]: MonadError[?[_], Throwable], A](
      kind: SchedulerType,
      res: => Resource[F, Either[InitializationError[Json], A]])
      : EitherT[Resource[F, ?], CreateError[Json], A] = {
    EitherT(linkScheduler(kind, res)).leftMap(ie => ie: CreateError[Json])
  }
}
