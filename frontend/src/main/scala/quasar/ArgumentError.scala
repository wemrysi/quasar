/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar

import slamdata.Predef._

import monocle.Prism
import scalaz.NonEmptyList
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.show._
import scalaz.syntax.std.option._
import shapeless._

sealed abstract class ArgumentError extends Product with Serializable {
  def message: String
}

object ArgumentError {
  final case class InvalidArgumentError(message: String)
      extends ArgumentError

  final case class InvalidStringCoercionError(str: String, expected: NonEmptyList[String])
      extends ArgumentError {
    def message = {
      val expectedString =
        if (expected.tail.isEmpty)
          s"“${expected.head}”"
        else
          s"one of " + expected.toList.mkString("“", ", ", "”")

      s"Expected $expectedString but found “$str”."
    }
  }

  final case class TemporalFormatError[N <: Nat](func: GenericFunc[N], str: String, hint: Option[String])
      extends ArgumentError {
    def message = s"Unable to parse '$str' as ${func.shows}." + ~hint.map(" (" + _ + ")")
  }

  final case class TypeError(error: UnificationError)
      extends ArgumentError {
    def message = error.message
  }

  val invalidArgumentError: Prism[ArgumentError, String] =
    Prism.partial[ArgumentError, String] {
      case InvalidArgumentError(s) => s
    } (InvalidArgumentError)

  val invalidStringCoercionError: Prism[ArgumentError, (String, NonEmptyList[String])] =
    Prism.partial[ArgumentError, (String, NonEmptyList[String])] {
      case InvalidStringCoercionError(s, ss) => (s, ss)
    } {
      case (s, ss) => InvalidStringCoercionError(s, ss)
    }

  def temporalFormatError[N <: Nat](func: GenericFunc[N], str: String, hint: Option[String]): ArgumentError =
    TemporalFormatError(func, str, hint)

  val typeError: Prism[ArgumentError, UnificationError] =
    Prism.partial[ArgumentError, UnificationError] {
      case TypeError(e) => e
    } (TypeError)
}
