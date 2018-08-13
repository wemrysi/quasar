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

package quasar.yggdrasil

import quasar.precog.common.CValue
import quasar.yggdrasil.table.{CF1, CF2}

trait FNModule {

  trait F2Like {
    def applyl(cv: CValue): CF1
    def applyr(cv: CValue): CF1
    def andThen(f1: CF1): CF2
  }

  implicit def liftF2(f: CF2) = new F2Like {
    def applyl(cv: CValue): CF1 = f.partialLeft(cv)
    def applyr(cv: CValue): CF1 = f.partialRight(cv)

    def andThen(f1: CF1): CF2 = CF2 { (c1, c2) =>
      f(c1, c2) flatMap f1.apply
    }
  }
}
