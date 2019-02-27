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

package quasar.qsu

import quasar.Qspec
import quasar.ejson

import matryoshka.data.Fix

object QProvSpec extends Qspec {

  val QP = QProv[Fix]
  val P = QP.prov
  val J = ejson.Fixed[Fix]

  "project path on empty dims creates new" >> {
    QP.projectPath(J.str("q"), D.empty) must_= Dimensions.origin(P.project(J.str("q"), ???))
  }

}
