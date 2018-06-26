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

import slamdata.Predef.{String, List}

import quasar.api._
import quasar.api.DataSourceError.InitializationError
import quasar.Condition
import quasar.fp.numeric.Positive
import quasar.impl.datasources.MockDataSourcesSpec.DefaultM

import eu.timepit.refined.auto._
import scalaz.{IMap, ISet, Id, StateT, ~>}, Id.Id
//import scalaz.std.string._
import cats.effect.IO
import shims._

final class MockDataSourcesSpec /*extends DataSourcesSpec[DefaultM, String]*/ {

  def datasources: DataSources[DefaultM, String] = quasar.api.MockDataSources[DefaultM, String](acceptedSet, errorCondition)
  def supportedType = DataSourceType("s3", 3L)
  def validConfigs = ("one", "five")
  def run: ~>[DefaultM, Id.Id] = λ[DefaultM ~> Id]( _.eval(IMap.empty).unsafeRunSync )

  val s3: DataSourceType    = DataSourceType("s3", Positive(3).get)
  val azure: DataSourceType = DataSourceType("azure", Positive(3).get)
  val mongo: DataSourceType = DataSourceType("mongodb", Positive(3).get)
  val acceptedSet: ISet[DataSourceType] = ISet.fromList(List(s3, azure, mongo))
  def errorCondition[C](
      rn: ResourceName,
      dst: DataSourceType,
      config: C
      ): Condition[InitializationError[C]] = Condition.normal()

}

object MockDataSourcesSpec {
  type Store = IMap[ResourceName, (DataSourceMetadata, String)]
  type DefaultM[A] = StateT[IO, Store, A]
}
