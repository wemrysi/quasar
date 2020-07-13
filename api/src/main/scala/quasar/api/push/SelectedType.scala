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

import java.lang.String
import scala._
import scala.collection.immutable.List

import cats.{Eq, Show}
import cats.implicits._

import skolems.∃

final case class SelectedType(index: TypeIndex, args: List[∃[Actual]])

object SelectedType {
  private def exEq(pair: (∃[Actual], ∃[Actual])): Boolean = pair match {
    case (∃(P.Boolean(b1)), ∃(P.Boolean(b2))) =>
      b1 === b2
    case (∃(P.Integer(i1)), ∃(P.Integer(i2))) =>
      i1 === i2
    case (∃(P.EnumSelect(s1)), ∃(P.EnumSelect(s2))) =>
      s1 === s2
    case _ =>
      false
  }

  private val exToString: ∃[Actual] => String = {
    case ∃(P.Boolean(b)) => s"$b"
    case ∃(P.Integer(i)) => s"$i"
    case ∃(P.EnumSelect(s)) => s"EnumSelect($s)"
    case ∃(P.Enum(_)) => s"Enum(?)"
  }

  implicit val selectedTypeEq: Eq[SelectedType] =
    Eq.instance {
      case (SelectedType(i1, as1), SelectedType(i2, as2)) =>
        i1 === i2 && as1.length === as2.length && as1.zip(as2).forall(exEq)
    }

  implicit val selectedTypeShow: Show[SelectedType] =
    Show show {
      case SelectedType(i, as) =>
        s"SelectedType(${i.show}, List(${as.map(exToString).intercalate(", ")}))"
    }
}
