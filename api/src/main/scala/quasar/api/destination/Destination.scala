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

package quasar.api.destination

import cats.data.NonEmptyList

import monocle.Prism

import quasar.api.destination.param._
import quasar.api.table.ColumnType

import java.lang.String
import scala.{Int, List}
import scala.util.Either

import skolems.∃

/**
 * @see quasar.api.destination.UntypedDestination
 */
trait Destination[F[_]] {
  type Type
  type TypeId

  val typeIdOrdinal: Prism[Int, TypeId]

  implicit val typeIdLabel: Label[TypeId]

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId]

  // Allows for both reporting available coercions and generalized validation
  def params(id: TypeId): List[Labeled[∃[TParam]]]

  // Will only be called with validated params, still some partiality though
  def construct(id: TypeId, params: List[∃[TArg]]): Either[String, Type]

  def destinationType: DestinationType

  def sinks: NonEmptyList[ResultSink[F, Type]]
}
