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

import quasar.api.ColumnType
import quasar.api.push.param.ParamError
import quasar.api.resource.ResourcePath

import cats.data.NonEmptyList

sealed trait ResultPushError[+D] extends Product with Serializable

object ResultPushError {
  final case class DestinationNotFound[D](destinationId: D)
      extends ResultPushError[D]

  final case class PushNotFound[D](destinationId: D, path: ResourcePath)
      extends ResultPushError[D]

  final case class PushAlreadyRunning[D](destinationId: D, path: ResourcePath)
      extends ResultPushError[D]

  final case class FullNotSupported[D](destinationId: D)
      extends ResultPushError[D]

  final case class IncrementalNotSupported[D](destinationId: D)
      extends ResultPushError[D]

  final case class InvalidCoercion[D](
      destinationId: D,
      column: String,
      scalar: ColumnType.Scalar,
      typeIndex: TypeIndex)
      extends ResultPushError[D]

  final case class TypeNotFound[D](
      destinationId: D,
      column: String,
      index: TypeIndex)
      extends ResultPushError[D]

  final case class TypeConstructionFailed[D](
      destinationId: D,
      column: String,
      typeLabel: String,
      errors: NonEmptyList[ParamError])
      extends ResultPushError[D]
}
