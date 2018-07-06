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

import slamdata.Predef.{Int, List, String, Stream => SStream}
import quasar.Condition
import quasar.contrib.scalaz.MonadState_

import cats.effect.IO
import cats.data.StateT
import eu.timepit.refined.auto._
import scalaz.ISet
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.string._
import shims._

import MockDatasourcesSpec._

final class MockDatasourcesSpec
  extends DatasourcesSpec[MockM, List, Int, String] {

  val s3: DatasourceType    = DatasourceType("s3", 1L)
  val azure: DatasourceType = DatasourceType("azure", 1L)
  val mongo: DatasourceType = DatasourceType("mongodb", 1L)
  val acceptedSet: ISet[DatasourceType] = ISet.fromList(List(s3, azure, mongo))

  def datasources: Datasources[MockM, List, Int, String] =
    MockDatasources[String, MockM, List](
      acceptedSet, _ => Condition.normal(), SStream.empty)

  def supportedType = DatasourceType("s3", 1L)

  def validConfigs = ("bucket1", "bucket2")
}

object MockDatasourcesSpec {
  import MockDatasources.MockState

  type MockM[A] = StateT[IO, MockState[String], A]

  implicit def scalazMonadMockState: MonadState_[MockM, MockState[String]] =
    new MonadState_[MockM, MockState[String]] {
      val get = StateT.get
      def put(ms: MockState[String]) = StateT.set(ms)
    }
}
