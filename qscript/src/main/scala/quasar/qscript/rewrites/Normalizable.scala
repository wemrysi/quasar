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

import quasar.RenderTreeT
import quasar.contrib.iota._
import quasar.qscript.{MapFuncCore, TTypes}

import matryoshka.{BirecursiveT, EqualT, ShowT}
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{Equal, Free, Show}

class NormalizableT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] extends TTypes[T] {

  def freeMF[A: Equal: Show](fm: Free[MapFunc, A]): Free[MapFunc, A] =
    fm.transCata[Free[MapFunc, A]](MapFuncCore.normalize[T, A])
}
