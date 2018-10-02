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

package quasar.mimir.evaluate

import quasar.precog.common.{CString, RArray, RObject, RValue}
import quasar.qscript._

import scalaz.syntax.std.option._
import scalaz.syntax.equal._
import scalaz.std.string._

object Shifting {

  final case class ShiftInfo(shiftPath: ShiftPath, idStatus: IdStatus, shiftKey: ShiftKey)

  /* Returns the `RObject` at the provided path, returning `None` when
   * the path points to a non-object or the path does not exist.
   *
   * A path `foo.bar.baz` is represented as `List("foo", "bar", "baz")`
   *
   * Paths including static array derefs are not currently supported.
   */
  def drillToObject(rvalue: RValue, path: List[String]): Option[RValue] = {
    (rvalue, path) match {
      case (v @ RObject(_), Nil) => v.some
      case (RObject(fields), head :: tail) =>
        val remainder: Option[(RValue, List[String])] =
          fields.collectFirst {
            case (key, target) if key === head => (target, tail)
          }
        remainder flatMap { case (target, tail) => drillToObject(target, tail) }
      case (_, _) => None
    }
  }

  /* Shifts the provided `RValue`, returning the shifted rows and omitting
   * unused fields.
   *
   * The empty list is returned when the input row is not required
   * to evaluate the query.
   */
  def shiftRValue(rvalue: RValue, shiftInfo: ShiftInfo): List[RValue] = {
    val shiftKey: String = shiftInfo.shiftKey.key
    val target: Option[RValue] = drillToObject(rvalue, shiftInfo.shiftPath.path)

    target match {
      case Some(RObject(fields)) => shiftInfo.idStatus match {
        case IdOnly =>
          fields.foldLeft(List[RValue]()) {
            case (acc, (k, _)) => RObject((shiftKey, CString(k))) :: acc
          }
        case IncludeId => // the qscript expects the results to be returned in an array
          fields.foldLeft(List[RValue]()) {
            case (acc, (k, v)) => RObject((shiftKey, RArray(CString(k), v))) :: acc
          }
        case ExcludeId =>
          fields.foldLeft(List[RValue]()) {
            case (acc, (_, v)) => RObject((shiftKey, v)) :: acc
          }
      }
      case _ =>  Nil // omit rows that cannot be shifted
    }
  }
}
