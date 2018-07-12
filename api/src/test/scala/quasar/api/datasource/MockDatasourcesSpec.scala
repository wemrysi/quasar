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

package quasar.api.datasource

import slamdata.Predef.{String, List}
import quasar.api.ResourceName

import quasar.Condition
import quasar.fp.numeric.Positive

import cats.effect.IO
import eu.timepit.refined.auto._
import scalaz.{IMap, ISet, Id, StateT, ~>}, Id.Id
import scalaz.std.string._
import shims._

import MockDatasourcesSpec.DefaultM

final class MockDatasourcesSpec extends DatasourcesSpec[DefaultM, String] {
  val s3: DatasourceType    = DatasourceType("s3", Positive(3).get)
  val azure: DatasourceType = DatasourceType("azure", Positive(3).get)
  val mongo: DatasourceType = DatasourceType("mongodb", Positive(3).get)
  val acceptedSet: ISet[DatasourceType] = ISet.fromList(List(s3, azure, mongo))

  def datasources: Datasources[DefaultM, String] =
    MockDatasources[DefaultM, String](acceptedSet, (_, _, _) => Condition.normal())

  def supportedType = DatasourceType("s3", 3L)

  def validConfigs = ("one", "five")

  def run: DefaultM ~> Id.Id =
    λ[DefaultM ~> Id]( _.eval(IMap.empty).unsafeRunSync )
}

object MockDatasourcesSpec {
  type Store = IMap[ResourceName, (DatasourceMetadata, String)]
  type DefaultM[A] = StateT[IO, Store, A]
}
