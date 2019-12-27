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

import quasar.{EffectfulQSpec, RateLimiter, RenderTreeT, ScalarStages}
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.impl.DatasourceModule
import quasar.impl.datasource.EmptyDatasource
import quasar.connector._
import quasar.connector.DataFormat
import quasar.contrib.scalaz._
import quasar.qscript.{MonadPlannerErr, PlannerError, InterpretedRead, QScriptEducated}

import fs2.Stream

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.{jEmptyObject, jString}

import cats.{Applicative, Show}
import cats.effect.{IO, ContextShift, ConcurrentEffect, Timer, Resource}
import cats.instances.int._
import cats.syntax.applicative._
import cats.syntax.applicativeError._

import eu.timepit.refined.auto._

import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data.Fix

import scalaz.{ISet, NonEmptyList}

import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import shims.{applicativeToScalaz, monoidKToScalaz, showToCats, showToScalaz}

object DatasourceModulesSpecextends extends EffectfulQSpec[IO] {
  implicit val tmr = IO.timer(global)

  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[Int, Json]].show(ce))

  implicit val ioPlannerErrorME: MonadError_[IO, PlannerError] =
    new MonadError_[IO, PlannerError] {
      def raiseError[A](e: PlannerError): IO[A] =
        IO.raiseError(new PlannerErrorException(e))

      def handleError[A](fa: IO[A])(f: PlannerError => IO[A]): IO[A] =
        fa.recoverWith {
          case PlannerErrorException(pe) => f(pe)
        }
    }

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))

      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  type R[F[_], A] = Either[InitializationError[Json], Datasource[F, Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]

  def mkDatasource[F[_]: Applicative, Q, P <: ResourcePathType](kind: DatasourceType)
      : Datasource[F, Stream[F, ?], Q, QueryResult[F], P] = {
    EmptyDatasource[F, Stream[F, ?], Q, QueryResult[F], P](
      kind,
      QueryResult.typed(
        DataFormat.ldjson,
        Stream.empty,
        ScalarStages.Id))
  }

  def lightMod(k: DatasourceType, err: Option[InitializationError[Json]] = None): DatasourceModule = DatasourceModule.Lightweight {
    new LightweightDatasourceModule {
      val kind = k
      def sanitizeConfig(config: Json): Json = jString("sanitized")
      def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
          config: Json,
          rateLimiter: RateLimiter[F])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, InterpretedRead[ResourcePath]]] = {
        val ds: R[F, InterpretedRead[ResourcePath]] = err match {
          case None => Right(mkDatasource(k))
          case Some(e) => Left(e)
        }
        Resource.make(ds.pure[F])(x => ().pure[F])
      }
    }
  }

  def heavyMod(k: DatasourceType, err: Option[InitializationError[Json]] = None) = DatasourceModule.Heavyweight {
    new HeavyweightDatasourceModule {
      val kind = k
      def sanitizeConfig(config: Json): Json = jString("sanitized")
      def heavyweightDatasource[
          T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
          F[_]: ConcurrentEffect: ContextShift: MonadPlannerErr: Timer](
          config: Json)(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, T[QScriptEducated[T, ?]]]] = {
        val ds: R[F, T[QScriptEducated[T, ?]]] = err match {
          case None => Right(mkDatasource(k))
          case Some(e) => Left(e)
        }
        Resource.make(ds.pure[F])(x => ().pure[F])
      }
    }
  }

  "supported types" >> {
    "empty" >>* {
      for {
        rl <- RateLimiter[IO](1.0)
        modules = DatasourceModules[Fix, IO, Int](List(), rl)
        tys <- modules.supportedTypes
      } yield {
        tys === ISet.empty
      }
    }
    "non-empty" >>* {
      for {
        rl <- RateLimiter[IO](1.0)
        modules = DatasourceModules[Fix, IO, Int](List(
          lightMod(DatasourceType("a", 1L)),
          lightMod(DatasourceType("b", 2L)),
          heavyMod(DatasourceType("c", 3L))),
          rl)
        tys <- modules.supportedTypes
      } yield {
        tys === ISet.fromList(List(DatasourceType("a", 1L), DatasourceType("b", 2L), DatasourceType("c", 3L)))
      }
    }
  }
  "sanitizing refs" >>* {
    val aType = DatasourceType("a", 1L)
    val bType = DatasourceType("b", 2L)
    val cType = DatasourceType("c", 3L)

    val aRef = DatasourceRef(aType, DatasourceName("a-name"), jString("a-config"))
    val bRef = DatasourceRef(bType, DatasourceName("b-name"), jString("b-config"))
    val cRef = DatasourceRef(cType, DatasourceName("c-name"), jString("c-config"))

    val aExpected = aRef.copy(config = jString("sanitized"))
    val bExpected = bRef.copy(config = jString("sanitized"))
    val cExpected = cRef.copy(config = jEmptyObject)

    RateLimiter[IO](1.0) map { (rl: RateLimiter[IO]) =>
      val modules = DatasourceModules[Fix, IO, Int](List(lightMod(aType), heavyMod(bType)), rl)
      modules.sanitizeRef(aRef) === aExpected
      modules.sanitizeRef(bRef) === bExpected
      modules.sanitizeRef(cRef) === cExpected
    }
  }

  "create" >> {
    val lightType = DatasourceType("light", 1L)
    val heavyType = DatasourceType("heavy", 2L)
    val incompatType = DatasourceType("incompat", 3L)

    val lightRef = DatasourceRef(lightType, DatasourceName("light-name"), jString("light-config"))
    val heavyRef = DatasourceRef(heavyType, DatasourceName("heavy-name"), jString("heavy-config"))
    val incompatRef = DatasourceRef(incompatType, DatasourceName("incompat-name"), jString("incompat-config"))


    "works with provided modules" >>* {
      for {
        rl <- RateLimiter[IO](1.0)
        modules = DatasourceModules[Fix, IO, Int](List(lightMod(lightType), heavyMod(heavyType)), rl)
        (light, finishL) <- modules.create(0, lightRef).allocated
        (heavy, finishH) <- modules.create(1, heavyRef).allocated
        _ <- finishL
        _ <- finishH
      } yield {
        light must beLike { case ManagedDatasource.ManagedLightweight(lw) => lw.kind === lightType }
        heavy must beLike { case ManagedDatasource.ManagedHeavyweight(hw) => hw.kind === heavyType }
      }
    }
    "errors with incompatible refs" >>* {
      for {
        rl <- RateLimiter[IO](1.0)
        modules = DatasourceModules[Fix, IO, Int](List(lightMod(lightType), heavyMod(heavyType)), rl)
        res <- modules.create(0, incompatRef).allocated.attempt
      } yield res must beLike {
        case Left(CreateErrorException(ce)) =>
          ce === DatasourceUnsupported(incompatType, ISet.empty.insert(lightType).insert(heavyType))
      }
    }
    "errors with initialization error" >>* {
      val malformed =
        DatasourceType("malformed", 1L)
      val malformedRef =
        DatasourceRef(malformed, DatasourceName("doesn't matter"), jString("malformed-config"))
      val invalid =
        DatasourceType("invalid", 1L)
      val invalidRef =
        DatasourceRef(invalid, DatasourceName("doesn't matter"), jString("invalid-config"))
      val connFailed =
        DatasourceType("conn-failed", 1L)
      val connFailedRef =
        DatasourceRef(connFailed, DatasourceName("doesn't matter"), jString("conn-failed-config"))
      val accessDenied =
        DatasourceType("access-denied", 1L)
      val accessDeniedRef =
        DatasourceRef(accessDenied, DatasourceName("doesn't matter"), jString("access-denied-config"))

      for {
        rl <- RateLimiter[IO](1.0)
        modules = DatasourceModules[Fix, IO, Int](List(
          lightMod(malformed, Some(MalformedConfiguration(malformed, jString("a"), "malformed configuration"))),
          heavyMod(invalid, Some(InvalidConfiguration(invalid, jString("b"), NonEmptyList("invalid configuration")))),
          lightMod(connFailed, Some(ConnectionFailed(connFailed, jString("c"), new Exception("conn failed")))),
          heavyMod(accessDenied, Some(AccessDenied(accessDenied, jString("d"), "access denied")))),
          rl)

        malformedDs <- modules.create(0, malformedRef).allocated.attempt
        invalidDs <- modules.create(1, invalidRef).allocated.attempt
        connFailedDs <- modules.create(2, connFailedRef).allocated.attempt
        accessDeniedDs <- modules.create(3, accessDeniedRef).allocated.attempt
      } yield {
        malformedDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === MalformedConfiguration(malformed, jString("a"), "malformed configuration")
        }
        invalidDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === InvalidConfiguration(invalid, jString("b"), NonEmptyList("invalid configuration"))
        }
        connFailedDs must beLike {
          case Left(CreateErrorException(ConnectionFailed(kind, config, cause))) =>
            kind === connFailed
            config === jString("c")
            cause.getMessage === "conn failed"
        }
        accessDeniedDs must beLike {
          case Left(CreateErrorException(ce)) =>
            ce === AccessDenied(accessDenied, jString("d"), "access denied")
        }
      }
    }
  }
}
