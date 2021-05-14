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

package quasar.connector.destination

import scala.{Option, None}
import cats.data.NonEmptyList

import monocle.Prism

import quasar.api.{ColumnType, Label}
import quasar.api.destination.DestinationType
import quasar.api.push.{SelectedType, TypeCoercion}

import scala.Int
import scala.util.Either

/**
 * @see quasar.api.destination.UntypedDestination
 */
trait Destination[F[_]] {
  type Type
  type TypeId

  val typeIdOrdinal: Prism[Int, TypeId]

  implicit val typeIdLabel: Label[TypeId]

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId]

  def construct(id: TypeId): Either[Type, Constructor[Type]]

  def destinationType: DestinationType

  def sinks: NonEmptyList[ResultSink[F, Type]]

  def defaultSelected(tpe: ColumnType.Scalar): Option[SelectedType] = None
}
