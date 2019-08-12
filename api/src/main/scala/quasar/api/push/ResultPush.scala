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

package quasar.api.push

import slamdata.Predef.{Option, Long, Unit}

import quasar.Condition
import quasar.api.destination.ResultType
import quasar.api.resource.ResourcePath

import scalaz.\/

/** @tparam F effects
  * @tparam T Table Id
  * @tparam D Destination Id
  */
trait ResultPush[F[_], TableId, DestinationId] {
  import ResultPushError._

  def start(tableId: TableId, destinationId: DestinationId, path: ResourcePath, format: ResultType[F], limit: Option[Long])
      : F[Condition[ResultPushError[TableId, DestinationId]]]

  def cancel(tableId: TableId)
      : F[Condition[ExistentialError[TableId, DestinationId]]]

  def status(tableId: TableId)
      : F[ResultPushError[TableId, DestinationId] \/ Status]

  def cancelAll: F[Unit]
}
