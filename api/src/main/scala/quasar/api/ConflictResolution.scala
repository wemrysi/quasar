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

package quasar.api

import slamdata.Predef.{Int, Some}

import scalaz.{Enum, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

sealed trait ConflictResolution {
  def fold[A](preserve: => A, replace: => A): A =
    this match {
      case ConflictResolution.Preserve => preserve
      case ConflictResolution.Replace => replace
    }
}

object ConflictResolution extends ConflictResolutionInstances {
  case object Preserve extends ConflictResolution
  case object Replace extends ConflictResolution

  val preserve: ConflictResolution =
    Preserve

  val replace: ConflictResolution =
    Replace
}

sealed abstract class ConflictResolutionInstances {
  implicit val enum: Enum[ConflictResolution] =
    new Enum[ConflictResolution] {
      def order(x: ConflictResolution, y: ConflictResolution) =
        toInt(x) ?|? toInt(y)

      def pred(cr: ConflictResolution) =
        cr.fold(ConflictResolution.replace, ConflictResolution.preserve)

      def succ(cr: ConflictResolution) =
        cr.fold(ConflictResolution.replace, ConflictResolution.preserve)

      override val min = Some(ConflictResolution.preserve)

      override val max = Some(ConflictResolution.replace)

      private def toInt(cr: ConflictResolution): Int =
        cr.fold(0, 1)
    }

  implicit val show: Show[ConflictResolution] =
    Show.showFromToString
}
