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

package quasar.impl.datasources

import slamdata.Predef.{Exception, None, Option, Some, Throwable, Unit}
import quasar.{Condition, Data, RenderTreeT}
import quasar.api.{DataSourceType, ResourceName, ResourcePath}
import quasar.api.DataSourceError.{CreateError, DataSourceUnsupported}
import quasar.connector.{DataSource, LightweightDataSourceModule, HeavyweightDataSourceModule}
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.{κ, κ2}
import quasar.fs.Planner.PlannerErrorME
import quasar.qscript.QScriptEducated

import argonaut.Json
import cats.effect.{Async, Concurrent}
import cats.effect.concurrent.Ref
import fs2.Stream
import fs2.async.{immutable, mutable}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{\/, EitherT, IMap, ISet, Monad, OptionT}
import scalaz.std.option._
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import shims._

final class DataSourceManagement[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: Async: PlannerErrorME,
    G[_]: Async] private (
    modules: F[DataSourceManagement.Modules],
    running: mutable.Signal[F, DataSourceManagement.Running[T, F, G]],
    errors: Ref[F, IMap[ResourceName, Exception]])
    extends DataSourceControl[F, Json]
    with DataSourceErrors[F] {

  import DataSourceManagement.reportCondition

  type DS = DataSourceManagement.DS[T, F, G]
  type Running = DataSourceManagement.Running[T, F, G]

  // DataSourceControl

  def init(
      name: ResourceName,
      config: DataSourceConfig[Json])
      : F[Condition[CreateError[Json]]] = {

    def init0(mod: LightweightDataSourceModule \/ HeavyweightDataSourceModule)
        : EitherT[F, CreateError[Json], DS] =
      mod.fold(
        lw => EitherT(lw.lightweightDataSource[F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.left),

        hw => EitherT(hw.heavyweightDataSource[T, F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.right))

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supported)

      mod <-
        OptionT(modules.map(_.lookup(config.kind)))
          .toRight[CreateError[Json]](DataSourceUnsupported(config.kind, sup))

      ds0 <- init0(mod)

      ds = ds0.bimap(
        reportCondition(reportErrors(name), _),
        reportCondition(reportErrors(name), _))

      _ <- EitherT.rightT(modifyAndShutdown(r => (r.insert(name, ds), r.lookupAssoc(name))))
    } yield ()

    inited.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  def shutdown(name: ResourceName): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(name, κ2(None)) match {
      case (v, m) => (m, v strengthL name)
    })

  def rename(src: ResourceName, dst: ResourceName): F[Unit] = {
    val renameRunning =
      modifyAndShutdown { r =>
        r.lookup(src) match {
          case Some(ds) =>
            ((r - src).insert(dst, ds), r.lookupAssoc(dst))

          case None =>
            (r, None)
        }
      }

    val renameErrors =
      errors.update(_.updateLookupWithKey(src, κ2(None)) match {
        case (v, m) => m.alter(dst, κ(v))
      })

    renameRunning *> renameErrors
  }

  val supported: F[ISet[DataSourceType]] =
    modules.map(_.keySet)


  // DataSourceErrors

  def errored: F[IMap[ResourceName, Exception]] =
    errors.get

  def lookup(name: ResourceName): F[Option[Exception]] =
    errors.get.map(_.lookup(name))


  ////

  private def reportErrors(name: ResourceName): Condition[Exception] => F[Unit] =
    cond => errors.update(_.alter(name, κ(Condition.optionIso.get(cond))))

  private def modifyAndShutdown(f: Running => (Running, Option[(ResourceName, DS)])): F[Unit] =
    for {
      t <- running.modify(f)

      _  <- t.traverse_ {
        case (n, ds) => errors.update(_ - n) *> shutdown0(ds)
      }
    } yield ()

  private def shutdown0(ds: DS): F[Unit] =
    ds.fold(_.shutdown, _.shutdown)
}

object DataSourceManagement {
  type Modules = IMap[DataSourceType, LightweightDataSourceModule \/ HeavyweightDataSourceModule]
  type LDS[F[_], G[_]] = DataSource[F, Stream[G, ?], ResourcePath, Stream[G, Data]]
  type HDS[T[_[_]], F[_], G[_]] = DataSource[F, Stream[G, ?], T[QScriptEducated[T, ?]], Stream[G, Data]]
  type DS[T[_[_]], F[_], G[_]] = LDS[F, G] \/ HDS[T, F, G]
  type Running[T[_[_]], F[_], G[_]] = IMap[ResourceName, DS[T, F, G]]

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: Concurrent: PlannerErrorME,
      G[_]: Async](
      modules: F[Modules])
      : F[(DataSourceControl[F, Json] with DataSourceErrors[F], immutable.Signal[F, Running[T, F, G]])] =
    for {
      runningS <- mutable.Signal[F, Running[T, F, G]](IMap.empty)
      errors <- Ref.of[F, IMap[ResourceName, Exception]](IMap.empty)
      ctrl = new DataSourceManagement[T, F, G](modules, runningS, errors)
    } yield (ctrl, runningS)

  def reportCondition[F[_]: Monad: MonadError_[?[_], Throwable], G[_], Q, R](
      f: Condition[Exception] => F[Unit],
      ds: DataSource[F, G, Q, R])
      : DataSource[F, G, Q, R] =
    new ConditionReportingDataSource[F, G, Q, R](f, ds)

  private final class ConditionReportingDataSource[
      F[_]: Monad: MonadError_[?[_], Throwable], G[_], Q, R](
      report: Condition[Exception] => F[Unit],
      underlying: DataSource[F, G, Q, R])
      extends DataSource[F, G, Q, R] {

    def kind = underlying.kind
    def shutdown = underlying.shutdown
    def evaluate(query: Q) = reportCondition(underlying.evaluate(query))
    def children(path: ResourcePath) = reportCondition(underlying.children(path))
    def descendants(path: ResourcePath) = reportCondition(underlying.descendants(path))
    def isResource(path: ResourcePath) = reportCondition(underlying.isResource(path))

    private def reportCondition[A](fa: F[A]): F[A] =
      MonadError_[F, Throwable].ensuring(fa) {
        case Some(ex: Exception) => report(Condition.abnormal(ex))
        case Some(other)         => ().point[F]
        case None                => report(Condition.normal())
      }
  }
}
