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

import slamdata.Predef.{Boolean, None, Option, Some, Throwable, Unit}
import quasar.Disposable
import quasar.api.datasource.DatasourceType
import quasar.common.resource._
import quasar.connector.Datasource

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

import ByNeedDatasource.NeedState

final class ByNeedDatasource[F[_], G[_], Q, R] private (
    datasourceType: DatasourceType,
    mvar: mutable.Queue[F, NeedState[F, Disposable[F, Datasource[F, G, Q, R]]]],
    scheduler: Scheduler)(
    implicit F: MonadError[F, Throwable])
    extends Datasource[F, G, Q, R] {

  val kind: DatasourceType = datasourceType

  def evaluate(query: Q): F[R] =
    getDatasource.flatMap(_.evaluate(query))

  def pathIsResource(path: ResourcePath): F[Boolean] =
    getDatasource.flatMap(_.pathIsResource(path))

  def prefixedChildPaths(path: ResourcePath)
      : F[Option[G[(ResourceName, ResourcePathType)]]] =
    getDatasource.flatMap(_.prefixedChildPaths(path))

  ////

  private def getDatasource: F[Datasource[F, G, Q, R]] =
    for {
      needState <- mvar.peek1

      ds <- needState match {
        case NeedState.Uninitialized(init) =>
          initAndGet

        case NeedState.Initialized(ds) =>
          ds.unsafeValue.pure[F]
      }
    } yield ds

  private def initAndGet: F[Datasource[F, G, Q, R]] =
    mvar.timedDequeue1(Duration.Zero, scheduler) flatMap {
      case Some(s @ NeedState.Uninitialized(init)) =>
        val doInit = for {
          ds <- init
          _  <- mvar.enqueue1(NeedState.Initialized(ds))
        } yield ds.unsafeValue

        F.handleErrorWith(doInit) { e =>
          mvar.enqueue1(s) *> F.raiseError(e)
        }

      case Some(s @ NeedState.Initialized(ds)) =>
        mvar.enqueue1(s).as(ds.unsafeValue)

      case None =>
        getDatasource
    }
}

object ByNeedDatasource {
  sealed trait NeedState[F[_], A]

  object NeedState {
    final case class Uninitialized[F[_], A](init: F[A]) extends NeedState[F, A]
    final case class Initialized[F[_], A](a: A) extends NeedState[F, A]
  }

  def apply[F[_]: Effect, G[_], Q, R](
      kind: DatasourceType,
      init: F[Disposable[F, Datasource[F, G, Q, R]]],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[Disposable[F, Datasource[F, G, Q, R]]] = {

    def dispose(q: mutable.Queue[F, NeedState[F, Disposable[F, Datasource[F, G, Q, R]]]]): F[Unit] =
      q.dequeue1 flatMap {
        case NeedState.Initialized(ds) => ds.dispose
        case _ => ().pure[F]
      }

    for {
      mvar <- async.boundedQueue[F, NeedState[F, Disposable[F, Datasource[F, G, Q, R]]]](1)(implicitly, pool)
      _    <- mvar.enqueue1(NeedState.Uninitialized(init))
    } yield Disposable(new ByNeedDatasource(kind, mvar, scheduler), dispose(mvar))
  }
}
