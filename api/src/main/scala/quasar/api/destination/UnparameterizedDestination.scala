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

import quasar.api.destination.param._

import java.lang.String
import scala.{List, Nil}
import scala.util.{Either, Right}

import skolems.∃

trait UnparameterizedDestination[F[_]] extends Destination[F] {
  type Type = TypeId

  def params(id: TypeId): List[Labeled[∃[TParam]]] = Nil

  def construct(id: TypeId, params: List[∃[TArg]]): Either[String, Type] = Right(id)
}
