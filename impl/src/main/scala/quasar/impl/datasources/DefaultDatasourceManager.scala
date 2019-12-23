/*
 * Copyright 2014–2019 SlamData Inc.
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

import slamdata.Predef._

import quasar.{RateLimiter, RenderTreeT}
import quasar.api.datasource.{DatasourceRef, DatasourceType}
import quasar.api.datasource.DatasourceError, DatasourceError._
import quasar.api.resource.ResourcePathType
import quasar.connector.{MonadResourceErr, QueryResult}
import quasar.contrib.scalaz._
import quasar.fp.ski.{κ, κ2}
import quasar.impl.DatasourceModule
import quasar.impl.IncompatibleModuleException.linkDatasource
import quasar.impl.datasource.MonadCreateErr
import quasar.qscript.MonadPlannerErr

import scala.concurrent.ExecutionContext

import argonaut.{Json, JsonScalaz}, JsonScalaz._
import argonaut.Argonaut.jEmptyObject

import cats.{ApplicativeError, MonadError}
import cats.effect.{ConcurrentEffect, ContextShift, Resource, Timer}
import cats.effect.concurrent.Ref
import cats.effect.syntax.bracket._
import cats.syntax.applicativeError._
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.{EitherT, IMap, ISet, OptionT, Order, Scalaz}
import Scalaz._
import shims.monadToScalaz

private[quasar] final class DefaultDatasourceManager[
    I: Order,
    T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
    F[_]: ConcurrentEffect: ContextShift: MonadCreateErr: MonadPlannerErr: MonadResourceErr: Timer,
    G[_],
    R,
    P <: ResourcePathType] private (
    modules: DefaultDatasourceManager.Modules,
    running: Ref[F, DefaultDatasourceManager.Running[I, T, F, G, R, P]],
    getRef: I => F[Option[DatasourceRef[Json]]],
    rateLimiter: RateLimiter[F],
    onCreate: (I, DefaultDatasourceManager.MDS[T, F, ResourcePathType.Physical]) => F[ManagedDatasource[T, F, G, R, P]])(
    implicit
    ec: ExecutionContext)
    extends DatasourceManager[I, Json, T, F, G, R, P] { self =>

  import DefaultDatasourceManager._

  type MDS = DefaultDatasourceManager.MDS[T, F, ResourcePathType.Physical]
  type Running = DefaultDatasourceManager.Running[I, T, F, G, R, P]
  type Allocated = DefaultDatasourceManager.Allocated[T, F, G, R, P]

  def managedDatasource(datasourceId: I, fallback: Option[DatasourceRef[Json]] = None): F[Option[ManagedDatasource[T, F, G, R, P]]] = for {
    mbCurrentRef <- getRef(datasourceId)
    oldDs <- running.get.map(_.lookup(datasourceId))
    currentRef = mbCurrentRef orElse fallback
    res <- (currentRef, oldDs) match {
      case (None, _) =>
        oldDs.traverse(x => shutdownDatasource(datasourceId)).map(κ(none))
      case (Some(ref), Some(allocated)) if DatasourceRef.atMostRenamed(ref, allocated.ref) =>
        allocated.datasource.some.point[F]
      case (Some(ref), _) => for {
        allocated <- MonadError_[F, CreateError[Json]].unattempt(mkDatasource(datasourceId, ref).run)
        _ <- updateAllocated(datasourceId, allocated)
      } yield allocated.datasource.some
    }
  } yield res

  def sanitizedRef(ref: DatasourceRef[Json]): DatasourceRef[Json] =
    modules.lookup(ref.kind)
      .fold(configL.set(jEmptyObject))(m => configL.modify(m.sanitizeConfig))
      .apply(ref)

  def shutdownDatasource(datasourceId: I): F[Unit] =
    modifyAndShutdown(_.updateLookupWithKey(datasourceId, κ2(None)).swap)

  val supportedDatasourceTypes: F[ISet[DatasourceType]] =
    modules.keySet.point[F]

  ////

  private val configL = DatasourceRef.config[Json]

  private def modifyAndShutdown(
      f: Running => (Running, Option[Allocated]))
      : F[Unit] =
    running.modify(f).flatMap(_.traverse_(_.finalizer))

  private def updateAllocated(datasourceId: I, allocated: Allocated): F[Unit] =
    modifyAndShutdown { r =>
      (r.insert(datasourceId, allocated), r.lookup(datasourceId))
    }

  private def mkDatasource(
      datasourceId: I,
      ref: DatasourceRef[Json])
      : EitherT[F, CreateError[Json], Allocated] = for {
    sup <-
      EitherT.rightU[CreateError[Json]](supportedDatasourceTypes)
    mod <-
      OptionT(modules.lookup(ref.kind).point[F])
        .toRight[CreateError[Json]](DatasourceUnsupported(ref.kind, sup))
    (mds, dispose) <-
      EitherT(MonadError_[F, CreateError[Json]].attempt(createDatasource(mod, ref).allocated))
    ds <-
      EitherT.rightT(onCreate(datasourceId, mds) onError { case _ => dispose })
  } yield Allocated(ref, ds, dispose)

  private def createDatasource(
      module: DatasourceModule,
      ref: DatasourceRef[Json])
      : Resource[F, MDS] = module match {
    case DatasourceModule.Lightweight(lw) =>
      handleInitErrors(module.kind, lw.lightweightDatasource[F](ref.config, rateLimiter))
        .map(ManagedDatasource.lightweight[T](_))

    case DatasourceModule.Heavyweight(hw) =>
      handleInitErrors(module.kind, hw.heavyweightDatasource[T, F](ref.config))
        .map(ManagedDatasource.heavyweight(_))
  }
}

private[quasar] object DefaultDatasourceManager {

  type Modules =
    IMap[DatasourceType, DatasourceModule]

  type MDS[T[_[_]], F[_], P <: ResourcePathType] =
    ManagedDatasource[T, F, Stream[F, ?], QueryResult[F], P]

  final case class Allocated[T[_[_]], F[_], G[_], R, P <: ResourcePathType](
      ref: DatasourceRef[Json],
      datasource: ManagedDatasource[T, F, G, R, P],
      finalizer:  F[Unit])
      extends Product with Serializable

  type Running[I, T[_[_]], F[_], G[_], R, P <: ResourcePathType] =
    IMap[I, Allocated[T, F, G, R, P]]

  final class Builder[
      I: Order,
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer,
      G[_],
      R,
      P <: ResourcePathType] private (
      onCreate: (I, MDS[T, F, ResourcePathType.Physical]) => F[ManagedDatasource[T, F, G, R, P]]) {

    def build(
        modules: Modules,
        getRef: I => F[Option[DatasourceRef[Json]]],
        rateLimiter: RateLimiter[F])(
        implicit
        ec: ExecutionContext)
        : Resource[F, DatasourceManager[I, Json, T, F, G, R, P]] = {

      val init = for {
        running <- Ref[F].of(IMap[I, Allocated[T, F, G, R, P]]())
        mgr = new DefaultDatasourceManager(modules, running, getRef, rateLimiter, onCreate)
      } yield (mgr, running)

      val resource = Resource.make(init) { case (mgr, running) =>
        running.get.flatMap { (mp: IMap[I, Allocated[T, F, G, R, P]]) =>
          mp.keySet
            .foldMapLeft1Opt(mgr.shutdownDatasource)((u, k) => u.guarantee(mgr.shutdownDatasource(k)))
            .sequence_
        }
      }
      resource.map(_._1)
    }

    def withMiddleware[H[_], S, Q <: ResourcePathType](
        f: (I, ManagedDatasource[T, F, G, R, P]) => F[ManagedDatasource[T, F, H, S, Q]])
        : Builder[I, T, F, H, S, Q] =
      new Builder[I, T, F, H, S, Q]((i, mds) => onCreate(i, mds).flatMap(f(i, _)))
  }

  object Builder {
    def apply[
        I: Order,
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: MonadResourceErr: MonadCreateErr: Timer]
        : Builder[I, T, F, Stream[F, ?], QueryResult[F], ResourcePathType.Physical] =
      new Builder[I, T, F, Stream[F, ?], QueryResult[F], ResourcePathType.Physical](
        (_, mds) => mds.point[F])
  }

  ////

  private def handleLinkageError[F[_]: ApplicativeError[?[_], Throwable], A](kind: DatasourceType, fa: => F[A]): F[A] =
    linkDatasource(kind, fa)

  private def handleInitErrors[F[_]: MonadCreateErr: MonadError[?[_], Throwable], A](
      kind: DatasourceType,
      res: => Resource[F, Either[InitializationError[Json], A]])
      : Resource[F, A] = {

    import quasar.contrib.cats.monadError.monadError_CatsMonadError

    implicit val merr: MonadError[F, CreateError[Json]] =
      monadError_CatsMonadError[F, CreateError[Json]](
        MonadError[F, Throwable], MonadError_[F, CreateError[Json]])

    val rmerr = MonadError[Resource[F, ?], CreateError[Json]]

    rmerr.rethrow(rmerr.map(handleLinkageError(kind, res))(_.leftMap(ie => ie: CreateError[Json])))
  }
}
