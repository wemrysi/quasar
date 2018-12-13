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

package quasar.qscript.rewrites

import slamdata.Predef.Option

import quasar.contrib.iota._
import quasar.qscript.{FreeMap, Hole, JoinFunc, LeftSide, RightSide, SrcHole}

import scalaz.std.option._
import scalaz.syntax.traverse._

/** Matches on a `JoinFunc` that never references `LeftSide`. */
object FocusedRepair {
  def unapply[T[_[_]]](jf: JoinFunc[T]): Option[FreeMap[T]] =
    jf traverse {
      case LeftSide => none[Hole]
      case RightSide => some(SrcHole)
    }
}
