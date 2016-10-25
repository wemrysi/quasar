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

package quasar.sql

import quasar.{Data, LogicalPlan => LP}
import quasar.Predef.String
import quasar.std.StdLib._

import matryoshka.Fix

sealed abstract class JoinDir(val name: String) {
  import structural.ObjectProject

  val data: Data = Data.Str(name)
  val const: Fix[LP] = Fix(LP.Constant(data))
  def projectFrom(lp: Fix[LP]): Fix[LP] = Fix(ObjectProject(lp, const))
}

object JoinDir {
  final case object Left extends JoinDir("left")
  final case object Right extends JoinDir("right")
}
