/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.yggdrasil.vfs

sealed trait MoveSemantics extends Product with Serializable {
  import MoveSemantics._

  /**
   * Given an existence check on the target, return whether or not
   * the operation is valid.
   */
  final def apply(check: Boolean): Boolean = this match {
    case Overwrite => true
    case FailIfExists => !check
    case FailIfMissing => check
  }
}

// mirrors ManageFile.MoveSemantics
object MoveSemantics {
  case object Overwrite extends MoveSemantics
  case object FailIfExists extends MoveSemantics
  case object FailIfMissing extends MoveSemantics
}
