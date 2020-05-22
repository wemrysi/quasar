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

import quasar.Condition
import quasar.api.scheduler._
import quasar.api.scheduler.SchedulerError._
import quasar.connector.scheduler.{Scheduler, SchedulerModule}
import quasar.impl.{CachedGetter, ResourceManager, IndexedSemaphore}, CachedGetter.Signal._
import quasar.impl.storage.IndexedStore

import argonaut.Json

import cats.data.{EitherT, OptionT}
import cats.effect._
import cats.implicits._
import cats.{~>, Eq}

private[impl] final class DefaultSchedulers[F[_]: Sync, I: Eq, II, C: Eq](
    semaphore: IndexedSemaphore[F, I],
    freshId: F[I],
    refs: IndexedStore[F, I, SchedulerRef[C]],
    cache: ResourceManager[F, I, Scheduler[F, II, Json]],
    getter: CachedGetter[F, I, SchedulerRef[C]],
    modules: SchedulerModules[F, II, C, Json])
    extends Schedulers[F, I, II, C, Json] {

  type Module = SchedulerModule
  type ModuleType = SchedulerType

  def enableModule(m: SchedulerModule): F[Unit] =
    modules.enable(m)

  def disableModule(m: SchedulerType): F[Unit] =
    modules.disable(m)

  def addScheduler(ref: SchedulerRef[C]): F[Either[CreateError[C], I]] = for {
    i <- freshId
    c <- addRef[CreateError[C]](i, ref)
  } yield Condition.eitherIso.get(c).as(i)

  def schedulerRef(i: I): F[Either[SchedulerNotFound[I], SchedulerRef[C]]] =
    OptionT(refs.lookup(i))
      .toRight(SchedulerNotFound(i))
      .semiflatMap(modules.sanitizeRef(_))
      .value

  def schedulerOf(i: I): F[Either[SchedulerError[I, C], Scheduler[F, II, Json]]] = {
    type SE = SchedulerError[I, C]

    lazy val error: F[Either[SE, Scheduler[F, II, Json]]] =
      (SchedulerNotFound(i): SE).asLeft[Scheduler[F, II, Json]].pure[F]

    def create(ref: SchedulerRef[C]): F[Either[SE, Scheduler[F, II, Json]]] = {
      val created = for {
        allocated <- allocateScheduler[SE](ref)
        _ <- EitherT.right[SE](cache.manage(i, allocated))
      } yield allocated._1

      created.value
    }

    def fromCacheOrCreate(ref: SchedulerRef[C]): F[Either[SE, Scheduler[F, II, Json]]] =
      OptionT(cache.get(i))
        .toRight[SE](SchedulerNotFound(i))
        .orElse(EitherT(create(ref)))
        .value

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          error

        case Removed(_) =>
          cache.shutdown(i) >> error

        case Updated(incoming, old) if !SchedulerRef.atMostRenamed(incoming, old) =>
          cache.shutdown(i) >> create(incoming)

        case exists: Exists[SchedulerRef[C]] =>
          fromCacheOrCreate(exists.value)
      }
    }
  }

  def removeScheduler(i: I): F[Condition[SchedulerError[I, C]]] =
    refs.delete(i).ifM(
      cache.shutdown(i).as(Condition.normal[SchedulerError[I, C]]()),
      Condition.abnormal(SchedulerNotFound(i): SchedulerError[I, C]).pure[F])

  def replaceScheduler(i: I, ref: SchedulerRef[C]): F[Condition[SchedulerError[I, C]]] = {
    lazy val notFound =
      Condition.abnormal(SchedulerNotFound(i): SchedulerError[I, C])

    def doReplace(prev: SchedulerRef[C]): F[Condition[SchedulerError[I, C]]] =
      if (ref === prev)
        Condition.normal[SchedulerError[I, C]]().pure[F]
      else if (SchedulerRef.atMostRenamed(ref, prev))
        setRef(i, ref)
      else
        addRef[SchedulerError[I, C]](i, ref)

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          notFound.pure[F]

        case Removed(_) =>
          cache.shutdown(i) as notFound

        case Updated(_, old) =>
          doReplace(old)

        case exists: Exists[SchedulerRef[C]] =>
          doReplace(exists.value)
      }
    }
  }

  def supportedTypes: F[Set[SchedulerType]] =
    modules.supportedTypes

  private def addRef[E >: CreateError[C] <: SchedulerError[I, C]](i: I, ref: SchedulerRef[C]): F[Condition[E]] = {
    val action = for {
      _ <- verifyNameUnique[E](ref.name, i)
      mbCurrent <- EitherT.right[E](cache.get(i))
      _ <- EitherT.right[E](mbCurrent.fold(().pure[F])(_ => cache.shutdown(i)))
      allocated <- allocateScheduler[E](ref)
      _ <- EitherT.right[E](refs.insert(i, ref))
      _ <- EitherT.right[E](cache.manage(i, allocated))
    } yield ()
    action.value.map(Condition.eitherIso.reverseGet(_))
  }

  private def setRef(i: I, ref: SchedulerRef[C]): F[Condition[SchedulerError[I, C]]] = {
    val action = for {
      _ <- verifyNameUnique[SchedulerError[I, C]](ref.name, i)
      _ <- EitherT.right[SchedulerError[I, C]](refs.insert(i, ref))
    } yield ()
    action.value.map(Condition.eitherIso.reverseGet(_))
  }

  private def verifyNameUnique[E >: CreateError[C] <: SchedulerError[I, C]](name: String, i: I): EitherT[F, E, Unit] =
    EitherT {
      refs.entries
        .exists(t => t._2.name === name && t._1 =!= i)
        .compile
        .fold(false)(_ || _)
        .map(x => if (x) Left(SchedulerNameExists(name): E) else Right(()))
    }

  private def throughSemaphore(i: I): F ~> F =
    λ[F ~> F](fa => semaphore.get(i).use(_ => fa))

  private def allocateScheduler[E >: CreateError[C] <: SchedulerError[I, C]](
      ref: SchedulerRef[C])
      : EitherT[F, E, (Scheduler[F, II, Json], F[Unit])] =
    EitherT(modules.create(ref).value.allocated flatMap {
      case (Left(e), finalize) => finalize.as((e: E).asLeft[(Scheduler[F, II, Json], F[Unit])])
      case (Right(a), finalize) => (a, finalize).asRight[E].pure[F]
    })
}

object DefaultSchedulers {
  private[impl] def apply[F[_]: Concurrent: ContextShift, I: Eq, II, C: Eq](
      freshId: F[I],
      refs: IndexedStore[F, I, SchedulerRef[C]],
      cache: ResourceManager[F, I, Scheduler[F, II, Json]],
      modules: SchedulerModules[F, II, C, Json])
      : F[DefaultSchedulers[F, I, II, C]] = for {
    semaphore <- IndexedSemaphore[F, I]
    getter <- CachedGetter(refs.lookup(_))
  } yield new DefaultSchedulers(semaphore, freshId, refs, cache, getter, modules)
}
