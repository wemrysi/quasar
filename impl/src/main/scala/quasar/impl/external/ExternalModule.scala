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

package quasar.impl.external

import quasar.impl.DatasourceModule
import quasar.connector.destination.DestinationModule
import quasar.connector.datasource.{HeavyweightDatasourceModule, LightweightDatasourceModule}

import slamdata.Predef._

// this trait exists mostly because we can't have a sealed algebra that covers *all* modules, so we project one here
sealed trait ExternalModule extends Product with Serializable

object ExternalModule {

  val wrap: PartialFunction[AnyRef, ExternalModule] = {
    case lw: LightweightDatasourceModule =>
      Datasource(DatasourceModule.Lightweight(lw))

    case hw: HeavyweightDatasourceModule =>
      Datasource(DatasourceModule.Heavyweight(hw))

    case dm: DestinationModule =>
      Destination(dm)
  }

  final case class Datasource(mod: DatasourceModule) extends ExternalModule
  final case class Destination(mod: DestinationModule) extends ExternalModule
}
