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

package quasar.api.datasource

import slamdata.Predef._

import quasar.fp.ski.κ

import eu.timepit.refined.refineV
import eu.timepit.refined.api.Refined
import eu.timepit.refined.string.MatchesRegex
import monocle.Prism
import shapeless.{Witness => W}

package object datasource {
  type NameP = MatchesRegex[W.`"[a-zA-Z0-9-]+"`.T]
  type Name = String Refined NameP

  def stringName = Prism[String, Name](
    refineV[NameP](_).fold(κ(None), Some(_)))(_.value)
}
