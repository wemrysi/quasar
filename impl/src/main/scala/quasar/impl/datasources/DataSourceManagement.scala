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

import slamdata.Predef.{Exception, None, Option, Some, Unit}
import quasar.{Condition, Data, RenderTreeT}
import quasar.api.{DataSourceType, ResourceName, ResourcePath}
import quasar.api.DataSourceError.{CreateError, DataSourceUnsupported}
import quasar.connector.DataSource
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.{κ, κ2}
import quasar.fs.Planner.PlannerErrorME
import quasar.impl.DataSourceModule
import quasar.impl.datasource.{ByNeedDataSource, ConditionReportingDataSource, FailedDataSource}
import quasar.qscript.QScriptEducated

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import cats.effect.concurrent.Ref
import fs2.Stream
import fs2.async.{immutable, mutable}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{\/, EitherT, IMap, ISet, Monad, OptionT, Scalaz}, Scalaz._
import shims._

final class DataSourceManagement[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: PlannerErrorME: Timer,
    G[_]: ConcurrentEffect: Timer] private (
    modules: DataSourceManagement.Modules,
    errors: Ref[F, IMap[ResourceName, Exception]],
    running: mutable.Signal[F, DataSourceManagement.Running[T, F, G]])
    extends DataSourceControl[F, Json]
    with DataSourceErrors[F] {

  import DataSourceManagement.withErrorReporting

  type DS = DataSourceManagement.DS[T, F, G]
  type Running = DataSourceManagement.Running[T, F, G]

  // DataSourceControl

  def init(
      name: ResourceName,
      config: DataSourceConfig[Json])
      : F[Condition[CreateError[Json]]] = {

    val init0: DataSourceModule => EitherT[F, CreateError[Json], DS] = {
      case DataSourceModule.Lightweight(lw) =>
        EitherT(lw.lightweightDataSource[F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.left)

      case DataSourceModule.Heavyweight(hw) =>
        EitherT(hw.heavyweightDataSource[T, F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.right)
    }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supported)

      mod <-
        OptionT(modules.lookup(config.kind).point[F])
          .toRight[CreateError[Json]](DataSourceUnsupported(config.kind, sup))

      ds0 <- init0(mod)

      ds = ds0.bimap(
        withErrorReporting(errors, name, _),
        withErrorReporting(errors, name, _))

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
    modules.keySet.point[F]


  // DataSourceErrors

  def errored: F[IMap[ResourceName, Exception]] =
    errors.get

  def lookup(name: ResourceName): F[Option[Exception]] =
    errors.get.map(_.lookup(name))


  ////

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
  type Modules = IMap[DataSourceType, DataSourceModule]
  type LDS[F[_], G[_]] = DataSource[F, Stream[G, ?], ResourcePath, Stream[G, Data]]
  type HDS[T[_[_]], F[_], G[_]] = DataSource[F, Stream[G, ?], T[QScriptEducated[T, ?]], Stream[G, Data]]
  type DS[T[_[_]], F[_], G[_]] = LDS[F, G] \/ HDS[T, F, G]
  type Running[T[_[_]], F[_], G[_]] = IMap[ResourceName, DS[T, F, G]]

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: PlannerErrorME: MonadError_[?[_], CreateError[Json]]: Timer,
      G[_]: ConcurrentEffect: Timer](
      modules: Modules,
      configured: IMap[ResourceName, DataSourceConfig[Json]])
      : F[(DataSourceControl[F, Json] with DataSourceErrors[F], immutable.Signal[F, Running[T, F, G]])] =
    for {
      errors <- Ref.of[F, IMap[ResourceName, Exception]](IMap.empty)

      assocs <- configured.toList traverse {
        case (name, cfg @ DataSourceConfig(kind, _)) =>
          modules.lookup(kind) match {
            case None =>
              val ds = FailedDataSource[CreateError[Json], F, Stream[G, ?], ResourcePath, Stream[G, Data]](
                kind,
                DataSourceUnsupported(kind, modules.keySet))

              (name, ds.left[HDS[T, F, G]]).point[F]

            case Some(mod) =>
              lazyDataSource[T, F, G](mod, cfg).strengthL(name)
          }
      }

      running = IMap.fromList(assocs) mapWithKey { (n, ds) =>
        ds.bimap(
          withErrorReporting(errors, n, _),
          withErrorReporting(errors, n, _))
      }

      runningS <- mutable.Signal[F, Running[T, F, G]](running)

      ctrl = new DataSourceManagement[T, F, G](modules, errors, runningS)
    } yield (ctrl, runningS)

  def withErrorReporting[F[_]: Monad: MonadError_[?[_], Exception], G[_], Q, R](
      errors: Ref[F, IMap[ResourceName, Exception]],
      name: ResourceName,
      ds: DataSource[F, G, Q, R])
      : DataSource[F, G, Q, R] =
    ConditionReportingDataSource[Exception, F, G, Q, R](
      c => errors.update(_.alter(name, κ(Condition.optionIso.get(c)))), ds)

  ////

  private def lazyDataSource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: PlannerErrorME: MonadError_[?[_], CreateError[Json]]: Timer,
      G[_]: ConcurrentEffect: Timer](
      module: DataSourceModule,
      config: DataSourceConfig[Json])
      : F[DS[T, F, G]] =
    module match {
      case DataSourceModule.Lightweight(lw) =>
        val mklw = MonadError_[F, CreateError[Json]] unattempt {
          lw.lightweightDataSource[F, G](config.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDataSource(config.kind, mklw).map(_.left)

      case DataSourceModule.Heavyweight(hw) =>
        val mkhw = MonadError_[F, CreateError[Json]] unattempt {
          hw.heavyweightDataSource[T, F, G](config.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDataSource(config.kind, mkhw).map(_.right)
    }
}
