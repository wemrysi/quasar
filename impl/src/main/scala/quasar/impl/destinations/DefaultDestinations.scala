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

package quasar.impl.destinations

import slamdata.Predef._

import quasar.Condition
import quasar.api.destination._
import quasar.api.destination.DestinationError._
import quasar.connector.destination.Destination
import quasar.impl.{CachedGetter, ResourceManager, IndexedSemaphore}, CachedGetter.Signal._
import quasar.impl.storage.IndexedStore

import cats.effect.{Concurrent, ContextShift, Sync}
import cats.effect.concurrent.Ref
import cats.~>

import fs2.Stream

import scalaz.{\/, -\/, \/-, Equal, EitherT, IMap, ISet, OptionT, Order}
import scalaz.syntax.monad._
import scalaz.syntax.either._
import scalaz.syntax.equal._
import scalaz.syntax.std.boolean._

import shims.{monadToScalaz, equalToCats}

private[impl] final class DefaultDestinations[F[_]: Sync, I: Order, C: Equal](
    semaphore: IndexedSemaphore[F, I],
    freshId: F[I],
    refs: IndexedStore[F, I, DestinationRef[C]],
    cache: ResourceManager[F, I, Destination[F]],
    getter: CachedGetter[F, I, DestinationRef[C]],
    modules: DestinationModules[F, C],
    currentErrors: Ref[F, IMap[I, Exception]])
    extends Destinations[F, Stream[F, ?], I, C] {

  def addDestination(ref: DestinationRef[C]): F[CreateError[C] \/ I] = for {
    i <- freshId
    c <- addRef[CreateError[C]](i, ref)
  } yield Condition.disjunctionIso.get(c).as(i)

  def allDestinationMetadata: F[Stream[F, (I, DestinationMeta)]] =
    Sync[F].pure {
      refs.entries.evalMap { case (i, ref) =>
        errorsOf(i).map(ex => (i, DestinationMeta.fromOption(ref.kind, ref.name, ex)))
      }
    }

  def destinationRef(i: I): F[ExistentialError[I] \/ DestinationRef[C]] =
    OptionT(refs.lookup(i))
      .toRight(destinationNotFound(i))
      .map(modules.sanitizeRef(_))
      .run

  def destinationOf(i: I): F[DestinationError[I, C] \/ Destination[F]] = {
    type DE = DestinationError[I, C]

    lazy val error: F[DE \/ Destination[F]] =
      destinationNotFound[I, DE](i).left[Destination[F]].pure[F]

    def create(ref: DestinationRef[C]): F[DE \/ Destination[F]] = {
      val created = for {
        allocated <- allocateDestination[DE](ref)
        _ <- cache.manage(i, allocated).liftM[EitherT[?[_], DE, ?]]
      } yield allocated._1

      created.run
    }

    def fromCacheOrCreate(ref: DestinationRef[C]): F[DE \/ Destination[F]] =
      OptionT(cache.get(i))
        .toRight(destinationNotFound[I, DE](i))
        .orElse(EitherT(create(ref)))
        .run

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          error

        case Removed(_) =>
          cache.shutdown(i) >> error

        case Updated(incoming, old) if !DestinationRef.atMostRenamed(incoming, old) =>
          cache.shutdown(i) >> create(incoming)

        case exists: Exists[DestinationRef[C]] =>
          fromCacheOrCreate(exists.value)
      }
    }
  }

  def destinationStatus(i: I): F[ExistentialError[I] \/ Condition[Exception]] =
    (refs.lookup(i) |@| errorsOf(i)) {
      case (Some(_), Some(ex)) => Condition.Abnormal(ex).right
      case (Some(_), None) => Condition.normal().right
      case _ => destinationNotFound(i).left
    }

  def removeDestination(i: I): F[Condition[ExistentialError[I]]] =
    refs.delete(i).ifM(
      cache.shutdown(i).as(Condition.normal[ExistentialError[I]]()),
      Condition.abnormal(destinationNotFound[I, ExistentialError[I]](i)).point[F])

  def replaceDestination(i: I, ref: DestinationRef[C]): F[Condition[DestinationError[I, C]]] = {
    lazy val notFound =
      Condition.abnormal(destinationNotFound[I, DestinationError[I, C]](i))

    def doReplace(prev: DestinationRef[C]): F[Condition[DestinationError[I, C]]] =
      if (ref === prev)
        Condition.normal[DestinationError[I, C]]().point[F]
      else if (DestinationRef.atMostRenamed(ref, prev))
        setRef(i, ref)
      else
        addRef[DestinationError[I, C]](i, ref)

    throughSemaphore(i) {
      getter(i) flatMap {
        case Empty =>
          notFound.point[F]

        case Removed(_) =>
          cache.shutdown(i) as notFound

        case Updated(_, old) =>
          doReplace(old)

        case exists: Exists[DestinationRef[C]] =>
          doReplace(exists.value)
      }
    }
  }

  def supportedDestinationTypes: F[ISet[DestinationType]] =
    modules.supportedTypes

  def errors: F[IMap[I, Exception]] =
    currentErrors.get

  private def errorsOf(i: I): F[Option[Exception]] =
    errors.map(_.lookup(i))

  private def addRef[E >: CreateError[C] <: DestinationError[I, C]](i: I, ref: DestinationRef[C])
      : F[Condition[E]] = {
    val action = for {
      _ <- verifyNameUnique[E](ref.name, i)

      mbCurrent <- EitherT.rightT(cache.get(i))
      _ <- EitherT.rightT(mbCurrent.fold(().point[F])(_ => cache.shutdown(i)))

      allocated <- allocateDestination[E](ref)

      _ <- EitherT.rightT(refs.insert(i, ref))
      _ <- EitherT.rightT(cache.manage(i, allocated))
    } yield ()

    action.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def setRef(i: I, ref: DestinationRef[C]): F[Condition[DestinationError[I, C]]] = {
    val action = for {
      _ <- verifyNameUnique[DestinationError[I, C]](ref.name, i)
      _ <- EitherT.rightT(refs.insert(i, ref))
    } yield ()
    action.run.map(Condition.disjunctionIso.reverseGet(_))
  }

  private def verifyNameUnique[E >: CreateError[C] <: DestinationError[I, C]](name: DestinationName, i: I): EitherT[F, E, Unit] =
    EitherT {
      refs.entries
        .exists(t => t._2.name === name && t._1 =/= i)
        .compile
        .fold(false)(_ || _)
        .map(_ ? destinationNameExists[E](name).left[Unit] | ().right)
    }

  private def allocateDestination[E >: CreateError[C] <: DestinationError[I, C]](
      ref: DestinationRef[C])
      : EitherT[F, E, (Destination[F], F[Unit])] =
    EitherT(modules.create(ref).run.allocated flatMap {
      case (-\/(e), finalize) => finalize.as((e: E).left[(Destination[F], F[Unit])])
      case (\/-(a), finalize) => (a, finalize).right[E].pure[F]
    })

  private def throughSemaphore(i: I): F ~> F =
    λ[F ~> F](fa => semaphore.get(i).use(_ => fa))
}

object DefaultDestinations {
  private[impl] def apply[F[_]: Concurrent: ContextShift, I: Order, C: Equal](
      freshId: F[I],
      refs: IndexedStore[F, I, DestinationRef[C]],
      cache: ResourceManager[F, I, Destination[F]],
      modules: DestinationModules[F, C])
      : F[DefaultDestinations[F, I, C]] = for {
    semaphore <- IndexedSemaphore[F, I]
    errs <- Ref.of[F, IMap[I, Exception]](IMap.empty)
    getter <- CachedGetter(refs.lookup(_))
  } yield new DefaultDestinations(semaphore, freshId, refs, cache, getter, modules, errs)
}
