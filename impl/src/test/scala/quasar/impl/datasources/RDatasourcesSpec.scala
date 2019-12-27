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

import quasar.{RateLimiter, ScalarStages}
import quasar.api.MockSchemaConfig
import quasar.api.datasource._
import quasar.api.datasource.DatasourceError._
import quasar.api.resource._
import quasar.concurrent.BlockingContext
import quasar.connector._
import quasar.connector.DataFormat
import quasar.contrib.scalaz._
import quasar.impl.{DatasourceModule, ResourceManager}
import quasar.impl.datasource.EmptyDatasource
import quasar.impl.storage.{IndexedStore, ConcurrentMapIndexedStore}
import quasar.qscript.{PlannerError, InterpretedRead}

import argonaut.Json
import argonaut.JsonScalaz._
import argonaut.Argonaut.jString

import cats.Show
import cats.effect.{IO, ConcurrentEffect, Resource, ContextShift, Timer}
import cats.effect.concurrent.Ref
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._
import cats.instances.string._

import eu.timepit.refined.auto._

import fs2.Stream

import matryoshka.data.Fix

import scalaz.IMap

import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

import skolems._
import shims.{orderToScalaz, showToScalaz, applicativeToScalaz, monoidKToScalaz, showToCats, monadToScalaz}

object RDatasourcesSpec extends DatasourcesSpec[IO, Stream[IO, ?], String, Json, MockSchemaConfig.type] {
  implicit val tmr = IO.timer(global)
  type PathType = ResourcePathType

  type Self = Datasources[IO, Stream[IO, ?], String, Json, MockSchemaConfig.type]
  type R[F[_], A] = Either[InitializationError[Json], Datasource[F, Stream[F, ?], A, QueryResult[F], ResourcePathType.Physical]]
  type MDS = ManagedDatasource[Fix, IO, Stream[IO, ?], QueryResult[IO], PathType]

  implicit val ioResourceErrorME: MonadError_[IO, ResourceError] =
    MonadError_.facet[IO](ResourceError.throwableP)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DatasourceError[String, Json]].show(ce))

  implicit val ioCreateErrorME: MonadError_[IO, CreateError[Json]] =
    new MonadError_[IO, CreateError[Json]] {
      def raiseError[A](e: CreateError[Json]): IO[A] =
        IO.raiseError(new CreateErrorException(e))

      def handleError[A](fa: IO[A])(f: CreateError[Json] => IO[A]): IO[A] =
        fa.recoverWith {
          case CreateErrorException(e) => f(e)
        }
    }

  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  implicit val ioPlannerErrorME: MonadError_[IO, PlannerError] =
    new MonadError_[IO, PlannerError] {
      def raiseError[A](e: PlannerError): IO[A] =
        IO.raiseError(new PlannerErrorException(e))

      def handleError[A](fa: IO[A])(f: PlannerError => IO[A]): IO[A] =
        fa.recoverWith {
          case PlannerErrorException(pe) => f(pe)
        }
    }

  def lightMod: DatasourceModule = DatasourceModule.Lightweight {
    new LightweightDatasourceModule {
      val kind = supportedType
      def sanitizeConfig(config: Json): Json = config

      def lightweightDatasource[F[_]: ConcurrentEffect: ContextShift: MonadResourceErr: Timer](
          config: Json,
          rateLimiter: RateLimiter[F])(
          implicit ec: ExecutionContext)
          : Resource[F, R[F, InterpretedRead[ResourcePath]]] = {
        val ds: R[F, InterpretedRead[ResourcePath]] =
          Right {
            EmptyDatasource[F, Stream[F, ?], InterpretedRead[ResourcePath], QueryResult[F], ResourcePathType.Physical](
              supportedType,
              QueryResult.typed(
                DataFormat.ldjson,
                Stream.empty,
                ScalarStages.Id))
          }
        Resource.make(ds.pure[F])(x => ().pure[F])
      }
    }
  }

  val pool: BlockingContext = BlockingContext.cached("rdatasources-spec")

  def datasources: Resource[IO, Self] = prepare.map(_._1)

  def prepare: Resource[IO, (Self, IndexedStore[IO, String, DatasourceRef[Json]], Ref[IO, List[String]], Ref[IO, List[String]])] = {
    val freshId = IO(java.util.UUID.randomUUID.toString())
    val fRefs: IO[IndexedStore[IO, String, DatasourceRef[Json]]] =
      IO(new ConcurrentHashMap[String, DatasourceRef[Json]]()).map { (mp: ConcurrentHashMap[String, DatasourceRef[Json]]) =>
        ConcurrentMapIndexedStore.unhooked[IO, String, DatasourceRef[Json]](mp, pool)
      }
    val rCache = ResourceManager[IO, String, ManagedDatasource[Fix, IO, Stream[IO, ?], QueryResult[IO], PathType]]
    val schema =
      new ResourceSchema[IO, MockSchemaConfig.type, (ResourcePath, QueryResult[IO])] {
        def apply(c: MockSchemaConfig.type, r: (ResourcePath, QueryResult[IO])) =
          MockSchemaConfig.MockSchema.pure[IO]
      }
    val errors = DatasourceErrors.fromMap((IMap(): IMap[String, Exception]).pure[IO])
    for {
      rateLimiter <- Resource.liftF(RateLimiter[IO](1.0))
      starts <- Resource.liftF(Ref.of[IO, List[String]](List()))
      shuts <- Resource.liftF(Ref.of[IO, List[String]](List()))

      modules = {
        DatasourceModules[Fix, IO, String](List(lightMod), rateLimiter)
          .widenPathType[PathType]
          .withMiddleware((i: String, mds: MDS) => starts.update(i :: _) as mds)
          .withFinalizer((i: String, mds: MDS) => shuts.update(i :: _))
      }
      refs <- Resource.liftF(fRefs)
      cache <- rCache
      result <- Resource.liftF {
        RDatasources[Fix, IO, String, Json, MockSchemaConfig.type, QueryResult[IO]](freshId, refs, modules, cache, errors, schema)
      }
    } yield (result, refs, starts, shuts)
  }


  def supportedType = DatasourceType("test-type", 3L)
  def validConfigs = (jString("one"), jString("two"))
  val schemaConfig = MockSchemaConfig
  def gatherMultiple[A](as: Stream[IO, A]) = as.compile.toList
}
