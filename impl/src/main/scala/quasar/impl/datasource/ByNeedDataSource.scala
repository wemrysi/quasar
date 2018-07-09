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

package quasar.impl.datasource

import slamdata.Predef.{Boolean, None, Some, Throwable, Unit}
import quasar.api._, ResourceError._
import quasar.connector.DataSource

import java.lang.IllegalStateException
import scala.Predef.implicitly
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import cats.MonadError
import cats.effect.Effect
import cats.syntax.applicative._
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import fs2.Scheduler
import fs2.async
import fs2.async.mutable
import scalaz.\/

import ByNeedDataSource.NeedState

final class ByNeedDataSource[F[_], G[_], Q, R] private (
    dataSourceType: DataSourceType,
    mvar: mutable.Queue[F, NeedState[F, DataSource[F, G, Q, R]]],
    scheduler: Scheduler)(
    implicit F: MonadError[F, Throwable])
    extends DataSource[F, G, Q, R] {

  val kind: DataSourceType = dataSourceType

  val shutdown: F[Unit] =
    for {
      needState <- mvar.dequeue1

      e <- F.attempt(needState match {
        case NeedState.Initialized(ds) => ds.shutdown
        case other => ().pure[F]
      })

      _ <- mvar.enqueue1(NeedState.Shutdown())

      _ <- F.fromEither(e)
    } yield ()

  def evaluate(query: Q): F[ReadError \/ R] =
    getDataSource.flatMap(_.evaluate(query))

  def children(path: ResourcePath): F[CommonError \/ G[(ResourceName, ResourcePathType)]] =
    getDataSource.flatMap(_.children(path))

  def isResource(path: ResourcePath): F[Boolean] =
    getDataSource.flatMap(_.isResource(path))

  ////

  private def alreadyShutdown[A]: F[A] =
    F.raiseError[A](new IllegalStateException("ByNeedDataSource: Shutdown"))

  private def getDataSource: F[DataSource[F, G, Q, R]] =
    for {
      needState <- mvar.peek1

      ds <- needState match {
        case NeedState.Uninitialized(init) =>
          initAndGet

        case NeedState.Initialized(ds) =>
          ds.pure[F]

        case NeedState.Shutdown() =>
          alreadyShutdown
      }
    } yield ds

  private def initAndGet: F[DataSource[F, G, Q, R]] =
    mvar.timedDequeue1(Duration.Zero, scheduler) flatMap {
      case Some(s @ NeedState.Uninitialized(init)) =>
        val doInit = for {
          ds <- init
          _  <- mvar.enqueue1(NeedState.Initialized(ds))
        } yield ds

        F.handleErrorWith(doInit) { e =>
          mvar.enqueue1(s) *> F.raiseError(e)
        }

      case Some(s @ NeedState.Initialized(ds)) =>
        mvar.enqueue1(s).as(ds)

      case Some(s @ NeedState.Shutdown()) =>
        mvar.enqueue1(s) *> alreadyShutdown

      case None =>
        getDataSource
    }
}

object ByNeedDataSource {
  sealed trait NeedState[F[_], A]

  object NeedState {
    final case class Uninitialized[F[_], A](init: F[A]) extends NeedState[F, A]
    final case class Initialized[F[_], A](a: A) extends NeedState[F, A]
    final case class Shutdown[F[_], A]() extends NeedState[F, A]
  }

  def apply[F[_]: Effect, G[_], Q, R](
      kind: DataSourceType,
      init: F[DataSource[F, G, Q, R]],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[DataSource[F, G, Q, R]] =
    for {
      mvar <- async.boundedQueue[F, NeedState[F, DataSource[F, G, Q, R]]](1)(implicitly, pool)
      _    <- mvar.enqueue1(NeedState.Uninitialized(init))
    } yield new ByNeedDataSource(kind, mvar, scheduler)
}
