/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.marklogic.qscript

import quasar.Predef._
import quasar.ejson.EJson
import quasar.fp.coproductShow
import quasar.physical.marklogic.ErrorMessages

import matryoshka.Fix
import monocle.Prism
import scalaz._
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.show._

sealed abstract class MarkLogicPlannerError

object MarkLogicPlannerError {
  final case class InvalidQName(strLit: String) extends MarkLogicPlannerError
  // TODO: If we had an enum for the date parts, this wouldn't be necessary.
  final case class UnrecognizedDatePart(name: String) extends MarkLogicPlannerError
  final case class UnrepresentableEJson(ejson: Fix[EJson], msgs: ErrorMessages) extends MarkLogicPlannerError
  final case class UnsupportedDatePart(name: String) extends MarkLogicPlannerError

  val invalidQName = Prism.partial[MarkLogicPlannerError, String] {
    case InvalidQName(s) => s
  } (InvalidQName)

  val unrecognizedDatePart = Prism.partial[MarkLogicPlannerError, String] {
    case UnrecognizedDatePart(name) => name
  } (UnrecognizedDatePart)

  val unrepresentableEJson = Prism.partial[MarkLogicPlannerError, (Fix[EJson], ErrorMessages)] {
    case UnrepresentableEJson(ejs, msgs) => (ejs, msgs)
  } (UnrepresentableEJson.tupled)

  val unsupportedDatePart = Prism.partial[MarkLogicPlannerError, String] {
    case UnsupportedDatePart(name) => name
  } (UnsupportedDatePart)

  implicit val show: Show[MarkLogicPlannerError] =
    Show.shows {
      case InvalidQName(s) =>
        s"'$s' is not a valid XML QName."

      case UnrecognizedDatePart(n) =>
        s"'$n' is not recognized as a date/time part."

      case UnrepresentableEJson(ejs, msgs) =>
        s"'${ejs.shows}' does not have an XQuery representation: ${msgs.intercalate(", ")}"

      case UnsupportedDatePart(n) =>
        s"Extracting '$n' from a date/time is not supported."
    }
}

