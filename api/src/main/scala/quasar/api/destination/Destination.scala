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
import scala.Int
import scala.util.Either

import skolems.∃

/**
 * @see quasar.api.destination.UntypedDestination
 */
trait Destination[F[_]] {
  type Type

  type Constructor[P] <: ConstructorLike[P]

  trait ConstructorLike[P] { self: Constructor[P] =>
    def apply(actual: P): Type
  }

  type TypeId

  val typeIdOrdinal: Prism[Int, TypeId]

  implicit val typeIdLabel: Label[TypeId]

  def coerce(tpe: ColumnType.Scalar): TypeCoercion[TypeId]

  def construct(id: TypeId): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]]

  def destinationType: DestinationType

  def sinks: NonEmptyList[ResultSink[F, Type]]

  // Convience function to consolidate all the type ascriptions
  protected def formalConstructor[A](ctor: Constructor[A], paramLabel: String, param: Formal[A])
      : ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]] =
    ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]((ctor, Labeled(paramLabel, param)))
}
