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

import slamdata.Predef._
import quasar.{ConditionMatchers, Data, RenderTreeT}
import quasar.api._
import quasar.api.DataSourceError.{CreateError, DataSourceUnsupported, InitializationError}
import quasar.api.ResourceError.{CommonError, ReadError}
import quasar.connector.{DataSource, HeavyweightDataSourceModule, LightweightDataSourceModule}
import quasar.contrib.scalaz.MonadError_
import quasar.fs.Planner.{PlannerError, PlannerErrorME}
import quasar.impl.DataSourceModule
import quasar.qscript.QScriptEducated

import java.lang.IllegalArgumentException
import scala.concurrent.ExecutionContext.Implicits.global

import argonaut.Json
import argonaut.JsonScalaz._
import cats.{Applicative, ApplicativeError}
import cats.effect.{ConcurrentEffect, IO, Timer}
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.functor._
import eu.timepit.refined.auto._
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data.Fix
import scalaz.{\/, IMap, Show}
import scalaz.syntax.either._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.std.anyVal._
import scalaz.std.option._
import shims._

final class DataSourceManagementSpec extends quasar.Qspec with ConditionMatchers {
  import DataSourceManagementSpec._

  type Mgmt = DataSourceControl[IO, Json] with DataSourceErrors[IO]
  type Running = DataSourceManagement.Running[Fix, IO, IO]

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

  val LightT = DataSourceType("light", 1L)
  val HeavyT = DataSourceType("heavy", 2L)

  val lightMod = new LightweightDataSourceModule {
    val kind = LightT

    def lightweightDataSource[
        F[_]: ConcurrentEffect: Timer,
        G[_]: ConcurrentEffect: Timer](
        config: Json)
        : F[InitializationError[Json] \/ DataSource[F, Stream[G, ?], ResourcePath, Stream[G, Data]]] =
      mkDataSource[ResourcePath, F, G](kind).right.pure[F]
  }

  val heavyMod = new HeavyweightDataSourceModule {
    val kind = HeavyT

    def heavyweightDataSource[
        T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
        F[_]: ConcurrentEffect: PlannerErrorME: Timer,
        G[_]: ConcurrentEffect: Timer](
        config: Json)
        : F[InitializationError[Json] \/ DataSource[F, Stream[G, ?], T[QScriptEducated[T, ?]], Stream[G, Data]]] =
      mkDataSource[T[QScriptEducated[T, ?]], F, G](kind).right.pure[F]
  }

  val modules: DataSourceManagement.Modules =
    IMap(
      lightMod.kind -> DataSourceModule.Lightweight(lightMod),
      heavyMod.kind -> DataSourceModule.Heavyweight(heavyMod))

  def withInitialMgmt[A](configured: IMap[ResourceName, DataSourceConfig[Json]])(f: (Mgmt, IO[Running]) => IO[A]): A =
    (for {
      t <- DataSourceManagement[Fix, IO, IO](modules, configured)
      (mgmt, run) = t
      a <- f(mgmt, run.get)
    } yield a).unsafeRunSync()

  def withMgmt[A](f: (Mgmt, IO[Running]) => IO[A]): A =
    withInitialMgmt(IMap.empty)(f)

  "control" >> {
    "init" >> {
      "creates a datasource successfully" >> withMgmt { (mgmt, running) =>
        for {
          r0 <- running
          c <- mgmt.init(ResourceName("foo"), DataSourceConfig(LightT, Json.jNull))
          r1 <- running
        } yield {
          r0.size must_= 0
          c must beNormal
          r1.member(ResourceName("foo")) must beTrue
        }
      }

      "errors when kind is unsupported" >> withMgmt { (mgmt, running) =>
        val unkT = DataSourceType("unknown", 1L)

        mgmt.init(ResourceName("foo"), DataSourceConfig(unkT, Json.jNull)) map { c =>
          c must beAbnormal(DataSourceUnsupported(unkT, modules.keySet))
        }
      }

      "finalizes an existing datasource when replaced" >> withMgmt { (mgmt, running) =>
        val a = ResourceName("a")

        for {
          c1 <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
          ds1 <- running.map(_.lookup(a))
          c2 <- mgmt.init(a, DataSourceConfig(HeavyT, Json.jNull))
          ds2 <- running.map(_.lookup(a))
        } yield {
          c1 must beNormal
          c2 must beNormal
          Applicative[Option].map2(ds1, ds2)(_ eq _) must beSome(false)
        }
      }

      "clears errors from an existing datasource when replaced" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.init(ResourceName("a"), DataSourceConfig(LightT, Json.jNull))
          ds <- running.map(_.lookup(ResourceName("a")))
          light = ds.flatMap(_.swap.toOption)
          _  <- light.traverse_(_.children(ResourcePath.root()).void).attempt
          e1 <- mgmt.lookup(ResourceName("a"))
          c2 <- mgmt.init(ResourceName("a"), DataSourceConfig(HeavyT, Json.jNull))
          r  <- running.map(_.lookup(ResourceName("a")))
          e2 <- mgmt.lookup(ResourceName("a"))
        } yield {
          c1 must beNormal
          e1 must beSome(beAnInstanceOf[IllegalArgumentException])
          c2 must beNormal
          r must beSome
          e2 must beNone
        }
      }
    }

    "shutdown" >> {
      "finalizes the datasource and clears errors" >> withMgmt { (mgmt, running) =>
        val a = ResourceName("a")

        for {
          c1 <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
          ds1 <- running.map(_.lookup(a))
          light = ds1.flatMap(_.swap.toOption)
          _  <- light.traverse_(_.children(ResourcePath.root()).void).attempt
          e1 <- mgmt.lookup(a)
          _ <- mgmt.shutdown(a)
          e2 <- mgmt.lookup(a)
          ds2 <- running.map(_.lookup(a))
        } yield {
          e1 must beSome
          e2 must beNone
          ds2 must beNone
        }
      }

      "no-op for a datasource that doesn't exist" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.init(ResourceName("a"), DataSourceConfig(LightT, Json.jNull))
          r1 <- running
          _ <- mgmt.shutdown(ResourceName("nope"))
          r2 <- running
        } yield {
          r1.member(ResourceName("a")) must beTrue
          r2.member(ResourceName("a")) must beTrue
        }
      }
    }

    "rename" >> {
      "no-op when src doesn't exist" >> withMgmt { (mgmt, running) =>
        for {
          c1 <- mgmt.init(ResourceName("a"), DataSourceConfig(LightT, Json.jNull))
          r1 <- running
          _ <- mgmt.rename(ResourceName("nope"), ResourceName("a"))
          r2 <- running
        } yield {
          r1.member(ResourceName("a")) must beTrue
          (r1 eq r2) must beTrue
        }
      }

      "updates the control handle" >> withMgmt { (mgmt, running) =>
        val (a, b) = (ResourceName("a"), ResourceName("b"))

        for {
          c1 <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
          r1 <- running
          _ <- mgmt.rename(a, b)
          r2 <- running
        } yield {
          r1.member(a) must beTrue
          r2.member(a) must beFalse
          Applicative[Option].map2(r1.lookup(a), r2.lookup(b))(_ eq _) must beSome(true)
        }
      }

      "preserves any associated errors" >> withMgmt { (mgmt, running) =>
        val (a, b) = (ResourceName("a"), ResourceName("b"))

        for {
          c <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
          ds1 <- running.map(_.lookup(a))
          light = ds1.flatMap(_.swap.toOption)
          _  <- light.traverse_(_.children(ResourcePath.root()).void).attempt
          e1 <- mgmt.lookup(a)
          _ <- mgmt.rename(a, b)
          e2 <- mgmt.lookup(b)
        } yield {
          c must beNormal
          e1 must beSome
          Applicative[Option].map2(e1, e2)(_ eq _) must beSome(true)
        }
      }

      "shutdown an existing destination" >> withMgmt { (mgmt, running) =>
        val (a, b) = (ResourceName("a"), ResourceName("b"))

        for {
          ca <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
          cb <- mgmt.init(a, DataSourceConfig(HeavyT, Json.jNull))
          dsa <- running.map(_.lookup(a))
          _ <- mgmt.rename(a, b)
          r <- running
        } yield {
          ca must beNormal
          cb must beNormal
          r.lookup(a) must beNone
          Applicative[Option].map2(dsa, r.lookup(b))(_ eq _) must beSome(true)
        }
      }
    }
  }

  "errors" >> {
    "lookup returns latest error" >> withMgmt { (mgmt, running) =>
      val a = ResourceName("a")
      val foo = ResourcePath.root() / ResourceName("foo")
      val bar = ResourcePath.root() / ResourceName("bar")

      for {
        ca <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
        ds1 <- running.map(_.lookup(a))
        light = ds1.flatMap(_.swap.toOption)
        _  <- light.traverse_(_.children(foo).void).attempt
        e1 <- mgmt.lookup(a)
        _  <- light.traverse_(_.children(bar).void).attempt
        e2 <- mgmt.lookup(a)
      } yield {
        ca must beNormal
        e1.exists(_.getMessage.contains("foo")) must beTrue
        e2.exists(_.getMessage.contains("bar")) must beTrue
      }
    }

    "lookup returns none once a previously erroring datasource succeeds" >> withMgmt { (mgmt, running) =>
      val a = ResourceName("a")
      val foo = ResourcePath.root() / ResourceName("foo")

      for {
        ca <- mgmt.init(a, DataSourceConfig(LightT, Json.jNull))
        ds1 <- running.map(_.lookup(a))
        light = ds1.flatMap(_.swap.toOption)
        _  <- light.traverse_(_.children(foo).void).attempt
        e1 <- mgmt.lookup(a)
        _  <- light.traverse_(_.isResource(foo).void).attempt
        e2 <- mgmt.lookup(a)
      } yield {
        ca must beNormal
        e1.exists(_.getMessage.contains("foo")) must beTrue
        e2 must beNone
      }
    }

    val initCfg: IMap[ResourceName, DataSourceConfig[Json]] =
      IMap(
        ResourceName("initlw") -> DataSourceConfig(LightT, Json.jNull),
        ResourceName("unsupp") -> DataSourceConfig(DataSourceType("nope", 1L), Json.jNull))

    "initial configured datasources report errors" >> withInitialMgmt(initCfg) { (mgmt, running) =>
      val initlw = ResourceName("initlw")
      val foo = ResourcePath.root() / ResourceName("foo")

      for {
        ds1 <- running.map(_.lookup(initlw))
        light = ds1.flatMap(_.swap.toOption)
        _  <- light.traverse_(_.children(foo).void).attempt
        e1 <- mgmt.lookup(initlw)
      } yield e1.exists(_.getMessage.contains("foo")) must beTrue
    }

    "initial configured datasources report config errors" >> withInitialMgmt(initCfg) { (mgmt, running) =>
      val unsupp = ResourceName("unsupp")
      val foo = ResourcePath.root() / ResourceName("foo")

      for {
        ds1 <- running.map(_.lookup(unsupp))
        light = ds1.flatMap(_.swap.toOption)
        _  <- light.traverse_(_.isResource(foo).void).attempt
        e1 <- mgmt.lookup(unsupp)
      } yield e1.exists(_.getMessage.contains("Unsupported")) must beTrue
    }
  }
}

object DataSourceManagementSpec {
  final case class PlannerErrorException(pe: PlannerError)
      extends Exception(pe.message)

  final case class CreateErrorException(ce: CreateError[Json])
      extends Exception(Show[DataSourceError[Json]].shows(ce))

  def mkDataSource[Q, F[_]: ApplicativeError[?[_], Throwable], G[_]](kind0: DataSourceType)
      : DataSource[F, Stream[G, ?], Q, Stream[G, Data]] =
    new DataSource[F, Stream[G, ?], Q, Stream[G, Data]] {
      val kind = kind0

      val shutdown = ().pure[F]

      def evaluate(query: Q): F[ReadError \/ Stream[G, Data]] =
        Stream.empty.covaryAll[G, Data].right[ReadError].pure[F]

      def children(path: ResourcePath): F[CommonError \/ Stream[G, (ResourceName, ResourcePathType)]] =
        ApplicativeError[F, Throwable]
          .raiseError(new IllegalArgumentException(s"InvalidPath: ${path.shows}"))

      def descendants(path: ResourcePath): F[CommonError \/ Stream[G, ResourcePath]] =
        Stream.empty.covaryAll[G, ResourcePath].right.pure[F]

      def isResource(path: ResourcePath): F[Boolean] =
        false.pure[F]
    }
}
