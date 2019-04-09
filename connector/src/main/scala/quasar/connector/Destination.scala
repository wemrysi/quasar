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

package quasar.connector

import slamdata.Predef.Unit
import quasar.api.resource._

/** @tparam F effects
  * @tparam G multiple results
  * @tparam I destination input
  */
trait Destination[F[_], G[_], I] {

  /** Returns a function within F that can consume input of type I, producing
    * side effects within G
    */
  def writeToPath(path: ResourcePath): F[I => G[Unit]]
}
