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

package quasar.impl.discovery

import quasar.EffectfulQSpec
import quasar.api.datasource._
import quasar.api.discovery._
import quasar.api.resource._
import quasar.connector.ResourceSchema
import quasar.impl.{EmptyDatasource, QuasarDatasource}

import scala.{Int, List, None, Option, Some}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.Either

import java.lang.String

import cats.effect.{IO, Resource}
import cats.implicits._

import matryoshka.data.Fix

import shims.{eqToScalaz, showToScalaz}

object DefaultDiscoverySpec extends EffectfulQSpec[IO] {

  import DiscoveryError._

  val lookup: String => IO[Option[QuasarDatasource[Fix, Resource[IO, ?], List, Int, ResourcePathType]]] = {
    case "extant" =>
      IO.pure(Some(QuasarDatasource.lightweight[Fix](EmptyDatasource(DatasourceType("testing", 1), 0))))

    case _ =>
      IO.pure(None)
  }

  val schema =
    new ResourceSchema[IO, MockSchemaConfig.type, (ResourcePath, Int)] {
      def apply(c: MockSchemaConfig.type, r: (ResourcePath, Int)) =
        MockSchemaConfig.MockSchema.pure[IO]
    }

  val discovery =
    DefaultDiscovery[Fix, IO, List, String, MockSchemaConfig.type, Int](lookup, schema)

  "path is resource" >> {
    discoveryExamples(discovery.pathIsResource(_, _))
  }

  "prefixed child paths" >> {
    discoveryExamples(discovery.prefixedChildPaths(_, _))
  }

  "resource schema" >> {
    discoveryExamples(discovery.resourceSchema(_, _, MockSchemaConfig))
  }

  def discoveryExamples[E >: DatasourceNotFound[String] <: DiscoveryError[String], A](
      f: (String, ResourcePath) => Resource[IO, Either[E, A]]) = {
    "error when datasource not found" >>* {
      f("nonextant", ResourcePath.root()).use(r => IO(r must beNotFound[E]("nonextant")))
    }

    "respond when datasource exists" >>* {
      f("extant", ResourcePath.root()).use(r => IO(r must not(beNotFound[E]("extant"))))
    }
  }

  def beNotFound[E >: DatasourceNotFound[String] <: DiscoveryError[String]](id: String) =
    beLeft[E](equal[DiscoveryError[String]](datasourceNotFound(id)) ^^ { (e: E) => e: DiscoveryError[String] })
}
