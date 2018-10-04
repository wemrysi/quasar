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

import quasar.precog.common.{CLong, CString, RArray, RObject, RValue}
import quasar.qscript._

import scalaz.syntax.equal._
import scalaz.syntax.std.option._

object Shifting {

  final case class ShiftInfo(
    shiftPath: ShiftPath,
    idStatus: IdStatus,
    shiftType: ShiftType,
    shiftKey: ShiftKey)

  /* Returns the `RObject` or `RArray` (determined by the `ShiftType`)
   * at the provided path, returning `None` when the path points to a
   * a non-composite or the path does not exist.
   *
   * A path `foo.bar.baz` is represented as `List("foo", "bar", "baz")`
   *
   * Paths including static array derefs are not currently supported.
   */
  def compositeValueAtPath(path: List[String], shiftType: ShiftType, rvalue: RValue)
      : Option[RValue] =
    (rvalue, path) match {
      case (v @ RObject(_), Nil) if shiftType === ShiftType.Map => v.some
      case (v @ RArray(_), Nil) if shiftType === ShiftType.Array => v.some

      case (RObject(fields), head :: tail) =>
        val remainder: Option[(RValue, List[String])] =
          fields.get(head).map((_, tail))

        remainder flatMap { case (target, tail) =>
          compositeValueAtPath(tail, shiftType, target)
        }

      case (_, _) => None
    }

  /* Shifts the provided `RValue`, returning the shifted rows and omitting
   * unused fields.
   *
   * The empty list is returned when the input row is not required
   * to evaluate the query.
   */
  def shiftRValue(rvalue: RValue, shiftInfo: ShiftInfo): List[RValue] = {
    val shiftKey: String = shiftInfo.shiftKey.key

    val target: Option[RValue] =
      compositeValueAtPath(shiftInfo.shiftPath.path, shiftInfo.shiftType, rvalue)

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

      case Some(RArray(elems)) => shiftInfo.idStatus match {
        case IdOnly =>
          0.until(elems.length).toList.map(idx => RObject((shiftKey, CLong(idx))))

        case IncludeId => // the qscript expects the results to be returned in an array
          val (_, res) = elems.foldLeft((0, List[RValue]())) {
            case ((idx, acc), elem) =>
              (idx + 1, RObject((shiftKey, RArray(CLong(idx), elem))) :: acc)
          }
          res.reverse

        case ExcludeId =>
          elems.map(elem => RObject((shiftKey, elem)))
      }

      case _ =>  Nil // omit rows that cannot be shifted
    }
  }
}
