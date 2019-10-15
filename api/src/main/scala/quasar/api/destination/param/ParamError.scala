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

package quasar.api.destination.param

import argonaut.Json

import scala.{Int, List, Option, Product, Serializable}

sealed trait ParamError extends Product with Serializable

object ParamError {
  final case class InvalidBoolean(json: Json) extends ParamError
  final case class InvalidInt(json: Json) extends ParamError
  final case class InvalidEnum(json: Json) extends ParamError
  final case class IntOutOfRange(i: Int, min: Option[Int], max: Option[Int]) extends ParamError
  final case class IntOutOfStep(i: Int, step: IntegerStep) extends ParamError
  final case class ValueNotInEnum[A](a: A, possibilities: List[A]) extends ParamError
}
