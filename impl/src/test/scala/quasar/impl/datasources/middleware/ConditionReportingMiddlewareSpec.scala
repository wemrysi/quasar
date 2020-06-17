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

package quasar.impl.datasources.middleware

import slamdata.Predef.{Boolean, List, None, Option, Unit}

import quasar.{Condition, ConditionMatchers, ScalarStages}
import quasar.api.datasource.DatasourceType
import quasar.api.resource._
import quasar.connector.datasource._
import quasar.impl.QuasarDatasource
import quasar.qscript.InterpretedRead

import java.lang.{Throwable, IllegalArgumentException}

import scala.concurrent.ExecutionContext.Implicits.global

import cats.data.NonEmptyList
import cats.effect.{IO, Resource}
import cats.effect.concurrent.Ref

object ConditionReportingMiddlewareSpec extends quasar.EffectfulQSpec[IO] with ConditionMatchers {

  type T[_[_]] = Unit

  val thatsRoot = new IllegalArgumentException("THAT'S ROOT!")

  object TestDs extends Datasource[Resource[IO, ?], List, InterpretedRead[ResourcePath], Unit, ResourcePathType] {
    val kind: DatasourceType = DatasourceType("tester", 7)

    val loaders = NonEmptyList.of(Loader.Batch(BatchLoader.Full { (ir: InterpretedRead[ResourcePath]) =>
      Resource liftF {
        if (ResourcePath.root.nonEmpty(ir.path))
          IO.raiseError(thatsRoot)
        else
          IO.pure(())
      }
    }))

    def pathIsResource(path: ResourcePath): Resource[IO, Boolean] =
      Resource.pure[IO, Boolean](false)

    def prefixedChildPaths(path: ResourcePath)
        : Resource[IO, Option[List[(ResourceName, ResourcePathType)]]] =
      Resource.pure[IO, Option[List[(ResourceName, ResourcePathType)]]](None)
  }

  val managedTester = QuasarDatasource.lightweight[T](TestDs)

  "initial condition is normal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Throwable]]())
      ds <- ConditionReportingMiddleware[IO, Unit, Throwable]((_, c) => r.update(c :: _))((), managedTester)
      cs <- r.get
    } yield {
      cs must_=== List(Condition.normal())
    }
  }

  "operations that succeed emit normal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Throwable]]())
      ds <- ConditionReportingMiddleware[IO, Unit, Throwable]((_, c) => r.update(c :: _))((), managedTester)
      _ <- ds.pathIsResource(ResourcePath.root()).use(IO.pure(_))
      cs <- r.get
    } yield {
      cs must_=== List(Condition.normal(), Condition.normal())
    }
  }

  "operations that throw emit abnormal" >>* {
    for {
      r <- Ref[IO].of(List[Condition[Throwable]]())
      ds <- ConditionReportingMiddleware[IO, Unit, Throwable]((_, c) => r.update(c :: _))((), managedTester)
      res = ds match {
        case QuasarDatasource.Lightweight(lw) => lw.loadFull(InterpretedRead(ResourcePath.root(), ScalarStages.Id)).value
        case _ => Resource.pure[IO, Unit](())
      }
      _ <- res.use(IO.pure(_)).attempt
      cs <- r.get
    } yield {
      cs must_=== List(Condition.abnormal(thatsRoot), Condition.normal())
    }
  }
}
