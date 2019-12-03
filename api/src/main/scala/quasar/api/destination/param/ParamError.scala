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

import cats.data.{Ior, NonEmptyList}

import java.lang.String
import scala.{Boolean, Int, Product, Serializable}

sealed trait ParamError extends Product with Serializable

object ParamError {
  final case class InvalidBoolean(value: Boolean, detail: String) extends ParamError
  final case class InvalidInt(value: Int, detail: String) extends ParamError
  final case class IntOutOfBounds(i: Int, bounds: Ior[Int, Int]) extends ParamError
  final case class IntOutOfStep(i: Int, step: IntegerStep) extends ParamError
  final case class ValueNotInEnum(selector: String, possibilities: NonEmptyList[String]) extends ParamError
}
