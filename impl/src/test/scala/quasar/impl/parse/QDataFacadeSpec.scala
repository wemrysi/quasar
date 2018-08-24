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

package quasar.impl.parse

import slamdata.Predef._

import quasar.Qspec
import quasar.common.data.Data

import jawn.{Facade, SupportParser}
import qdata.QDataEncode
import scalaz.syntax.std.option._

// TODO specs for QDataFacade and QDataPreciseFacade
object QDataFacadeSpec extends Qspec {
  def parser[J: QDataEncode]: SupportParser[J] = new SupportParser[J] {
    implicit def facade: Facade[J] = QDataFacade.qdata[J]
  }

  "facade parsing" >> {
    parser[Data].parseFromString("42").toOption must_===
      Data.Int(42).some

    parser[Data].parseFromString("42.17").toOption must_===
      Data.Dec(42.17).some

    parser[Data].parseFromString("""{"foo": true}""").toOption must_===
      Data.Obj(ListMap(("foo", Data.True))).some
  }
}
