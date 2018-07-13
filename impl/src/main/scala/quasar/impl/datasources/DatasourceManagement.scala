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
import quasar.api.{ResourceName, ResourcePath}
import quasar.api.datasource.DatasourceType
import quasar.api.datasource.DatasourceError.{CreateError, DatasourceUnsupported}
import quasar.connector.Datasource
import quasar.contrib.scalaz.MonadError_
import quasar.fp.ski.{κ, κ2}
import quasar.fs.Planner.PlannerErrorME
import quasar.impl.DatasourceModule
import quasar.impl.datasource.{ByNeedDatasource, ConditionReportingDatasource, FailedDatasource}
import quasar.qscript.QScriptEducated

import scala.concurrent.ExecutionContext

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import fs2.{Scheduler, Stream}
import fs2.async.{immutable, mutable, Ref}
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{\/, EitherT, IMap, ISet, Monad, OptionT, Scalaz}, Scalaz._
import shims._

final class DatasourceManagement[
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: PlannerErrorME: Timer,
    G[_]: ConcurrentEffect: Timer] private (
    modules: DatasourceManagement.Modules,
    errors: Ref[F, IMap[ResourceName, Exception]],
    running: mutable.Signal[F, DatasourceManagement.Running[T, F, G]])
    extends DatasourceControl[F, Json]
    with DatasourceErrors[F] {

  import DatasourceManagement.withErrorReporting

  type DS = DatasourceManagement.DS[T, F, G]
  type Running = DatasourceManagement.Running[T, F, G]

  // DatasourceControl

  def init(
      name: ResourceName,
      config: DatasourceConfig[Json])
      : F[Condition[CreateError[Json]]] = {

    val init0: DatasourceModule => EitherT[F, CreateError[Json], DS] = {
      case DatasourceModule.Lightweight(lw) =>
        EitherT(lw.lightweightDatasource[F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.left)

      case DatasourceModule.Heavyweight(hw) =>
        EitherT(hw.heavyweightDatasource[T, F, G](config.config))
          .bimap(ie => ie: CreateError[Json], _.right)
    }

    val inited = for {
      sup <- EitherT.rightU[CreateError[Json]](supported)

      mod <-
        OptionT(modules.lookup(config.kind).point[F])
          .toRight[CreateError[Json]](DatasourceUnsupported(config.kind, sup))

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
      errors.modify(_.updateLookupWithKey(src, κ2(None)) match {
        case (v, m) => m.alter(dst, κ(v))
      })

    renameRunning <* renameErrors
  }

  val supported: F[ISet[DatasourceType]] =
    modules.keySet.point[F]


  // DatasourceErrors

  def errored: F[IMap[ResourceName, Exception]] =
    errors.get

  def lookup(name: ResourceName): F[Option[Exception]] =
    errors.get.map(_.lookup(name))


  ////

  private def modifyAndShutdown(f: Running => (Running, Option[(ResourceName, DS)])): F[Unit] =
    for {
      t <- running.modify2(f)

      _  <- t._2.traverse_ {
        case (n, ds) => errors.modify(_ - n) *> shutdown0(ds)
      }
    } yield ()

  private def shutdown0(ds: DS): F[Unit] =
    ds.fold(_.shutdown, _.shutdown)
}

object DatasourceManagement {
  type Modules = IMap[DatasourceType, DatasourceModule]
  type LDS[F[_], G[_]] = Datasource[F, Stream[G, ?], ResourcePath, Stream[G, Data]]
  type HDS[T[_[_]], F[_], G[_]] = Datasource[F, Stream[G, ?], T[QScriptEducated[T, ?]], Stream[G, Data]]
  type DS[T[_[_]], F[_], G[_]] = LDS[F, G] \/ HDS[T, F, G]
  type Running[T[_[_]], F[_], G[_]] = IMap[ResourceName, DS[T, F, G]]

  def apply[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: PlannerErrorME: MonadError_[?[_], CreateError[Json]]: Timer,
      G[_]: ConcurrentEffect: Timer](
      modules: Modules,
      configured: IMap[ResourceName, DatasourceConfig[Json]],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[(DatasourceControl[F, Json] with DatasourceErrors[F], immutable.Signal[F, Running[T, F, G]])] = {

    implicit val ec: ExecutionContext = pool

    for {
      errors <- Ref[F, IMap[ResourceName, Exception]](IMap.empty)

      assocs <- configured.toList traverse {
        case (name, cfg @ DatasourceConfig(kind, _)) =>
          modules.lookup(kind) match {
            case None =>
              val ds = FailedDatasource[CreateError[Json], F, Stream[G, ?], ResourcePath, Stream[G, Data]](
                kind,
                DatasourceUnsupported(kind, modules.keySet))

              (name, ds.left[HDS[T, F, G]]).point[F]

            case Some(mod) =>
              lazyDatasource[T, F, G](mod, cfg, pool, scheduler).strengthL(name)
          }
      }

      running = IMap.fromList(assocs) mapWithKey { (n, ds) =>
        ds.bimap(
          withErrorReporting(errors, n, _),
          withErrorReporting(errors, n, _))
      }

      runningS <- mutable.Signal[F, Running[T, F, G]](running)

      ctrl = new DatasourceManagement[T, F, G](modules, errors, runningS)
    } yield (ctrl, runningS)
  }

  def withErrorReporting[F[_]: Monad: MonadError_[?[_], Exception], G[_], Q, R](
      errors: Ref[F, IMap[ResourceName, Exception]],
      name: ResourceName,
      ds: Datasource[F, G, Q, R])
      : Datasource[F, G, Q, R] =
    ConditionReportingDatasource[Exception, F, G, Q, R](
      c => errors.modify(_.alter(name, κ(Condition.optionIso.get(c)))).void, ds)

  ////

  private def lazyDatasource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: PlannerErrorME: MonadError_[?[_], CreateError[Json]]: Timer,
      G[_]: ConcurrentEffect: Timer](
      module: DatasourceModule,
      config: DatasourceConfig[Json],
      pool: ExecutionContext,
      scheduler: Scheduler)
      : F[DS[T, F, G]] =
    module match {
      case DatasourceModule.Lightweight(lw) =>
        val mklw = MonadError_[F, CreateError[Json]] unattempt {
          lw.lightweightDatasource[F, G](config.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(config.kind, mklw, pool, scheduler).map(_.left)

      case DatasourceModule.Heavyweight(hw) =>
        val mkhw = MonadError_[F, CreateError[Json]] unattempt {
          hw.heavyweightDatasource[T, F, G](config.config)
            .map(_.leftMap(ie => ie: CreateError[Json]))
        }

        ByNeedDatasource(config.kind, mkhw, pool, scheduler).map(_.right)
    }
}
