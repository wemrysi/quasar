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

package quasar.api.push

import slamdata.Predef._

import quasar.Condition
import quasar.api.ColumnType
import quasar.api.resource.ResourcePath

import cats.data.NonEmptyList

import skolems.∃

/** Provides the ability to push the scalar results of queries into destinations. */
trait ResultPush[F[_], DestinationId, Query] {
  import ResultPushError._

  /** Cancel execution of an active push. */
  def cancel(destinationId: DestinationId, path: ResourcePath)
      : F[Condition[DestinationNotFound[DestinationId]]]

  /** Cancel execution of all active pushes. */
  def cancelAll: F[Unit]

  /** Returns a specification of how the given scalar type may be represented in
    * the specified destination.
    */
  def coerce(destinationId: DestinationId, scalar: ColumnType.Scalar)
      : F[Either[DestinationNotFound[DestinationId], TypeCoercion[CoercedType]]]

  /** Returns the latest pushes made to the specified destination. */
  def pushedTo(destinationId: DestinationId)
      : F[Either[DestinationNotFound[DestinationId], Map[ResourcePath, ∃[Push[?, Query]]]]]

  /** Start a push to the specified destination.
    *
    * On success, this function returns immediately and provides an effect that
    * may be used to await termination of the push.
    *
    * @param limit restrict the number of rows pushed to this amount, does not
    *              affect subsequent `update`s of this push.
    */
  def start(
      destinationId: DestinationId,
      config: ∃[PushConfig[?, Query]],
      limit: Option[Long])
      : F[Either[NonEmptyList[ResultPushError[DestinationId]], F[Status.Terminal]]]

  /** Update the specified push to reflect the latest data from its source(s).
    *
    * For `Full` pushes, this will result in a full scan of all sources, replacing
    * the results in the destination.
    *
    * For `Incremental` pushes, this will result in updating the results in the
    * destination with the changes since the previous execution of the push.
    */
  def update(destinationId: DestinationId, path: ResourcePath)
      : F[Either[NonEmptyList[ResultPushError[DestinationId]], F[Status.Terminal]]]
}
