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

package quasar.connector

import quasar.{Data, RenderTreeT}
import quasar.api.DataSourceType
import quasar.api.DataSourceError.InitializationError
import quasar.fs.Planner.PlannerErrorME
import quasar.qscript.QScriptEducated

import argonaut.Json
import cats.effect.{ConcurrentEffect, Timer}
import fs2.Stream
import matryoshka.{BirecursiveT, EqualT, ShowT}
import scalaz.\/

trait HeavyweightDataSourceModule {
  def kind: DataSourceType

  def heavyweightDataSource[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      F[_]: ConcurrentEffect: PlannerErrorME: Timer,
      G[_]: ConcurrentEffect: Timer](
      config: Json)
      : F[InitializationError[Json] \/ DataSource[F, Stream[G, ?], T[QScriptEducated[T, ?]], Stream[G, Data]]]
}
