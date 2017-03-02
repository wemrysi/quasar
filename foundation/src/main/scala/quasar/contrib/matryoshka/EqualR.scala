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

package quasar.contrib.matryoshka

import slamdata.Predef._

import matryoshka._
import matryoshka.implicits._
import scalaz._

object EqualR {
  def equal[T, F[_]: Functor](a: T, b: T)(implicit T: Recursive.Aux[T, F], E: Delay[Equal, F]): Boolean =
    E(equalR[T, F](E)).equal(a.project, b.project)

  def equalR[T, F[_]: Functor](eqF: Delay[Equal, F])(implicit T: Recursive.Aux[T, F]): Equal[T] =
    Equal.equal[T](equal[T, F](_, _)(Functor[F], T, eqF))
}
