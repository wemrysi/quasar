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

package quasar.api.push

import quasar.api.push.param.{Actual, ParamType => P}

import scala.{None, Option, Some, StringContext}

import cats.{Eq, Show}
import cats.implicits._

import skolems.∃

final case class SelectedType(index: TypeIndex, arg: Option[∃[Actual]])

object SelectedType {
  implicit val selectedTypeEq: Eq[SelectedType] =
    Eq.instance {
      case (SelectedType(i1, None), SelectedType(i2, None)) =>
        i1 === i2

      case (SelectedType(i1, Some(∃(P.Boolean(b1)))), SelectedType(i2, Some(∃(P.Boolean(b2))))) =>
        i1 === i2 && b1 === b2

      case (SelectedType(i1, Some(∃(P.Integer(n1)))), SelectedType(i2, Some(∃(P.Integer(n2))))) =>
        i1 === i2 && n1 === n2

      case (SelectedType(i1, Some(∃(P.EnumSelect(s1)))), SelectedType(i2, Some(∃(P.EnumSelect(s2))))) =>
        i1 === i2 && s1 === s2

      case _ => false
    }

  implicit val selectedTypeShow: Show[SelectedType] =
    Show show {
      case SelectedType(i, a) =>
        val astr = a map {
          case ∃(P.Boolean(b)) => s"$b"
          case ∃(P.Integer(i)) => s"$i"
          case ∃(P.EnumSelect(s)) => s"EnumSelect($s)"
          case ∃(P.Enum(_)) => s"Enum(?)"
        }

        s"SelectedType(${i.show}, $astr)"
    }
}
