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
import quasar.common.data.{Data, DataGenerators}
import quasar.frontend.data.DataCodec
import quasar.pkg.tests._

import jawn.{Facade, SupportParser}
import qdata.QDataEncode
import scalaz.syntax.std.option._

object QDataFacadeSpec extends Qspec {

  def parser[J: QDataEncode]: SupportParser[J] = new SupportParser[J] {
    implicit def facade: Facade[J] = QDataFacade.qdata[J]
  }

  // does not generate temporal types or Data.NA
  def genAtomic: Gen[Data] =
    Gen.oneOf[Data](
      Data.Null,
      Data.True,
      Data.False,
      genUnicodeString ^^ Data.Str,
      DataGenerators.defaultInt ^^ Data.Int,
      DataGenerators.defaultDec ^^ Data.Dec)

  implicit val codec: DataCodec = DataCodec.Readable

  implicit val arbData: Arbitrary[Data] =
    Arbitrary(DataGenerators.genDataDefault(genAtomic))

  "readable json facade parsing" >> {

    "parse arbitrary" >> prop { (data: Data) =>
      val json: Option[String] = DataCodec.render(data)
      json.flatMap(parser[Data].parseFromString(_).toOption) must_=== data.some
    }.set(minTestsOk = 1000)

    // a concrete example
    "parse concrete" >> {
      val json: String = """{ "foo": true, "bar": [1, null, 2.3] }"""
      parser[Data].parseFromString(json).toOption must_===
        Data.Obj(ListMap(
          ("foo", Data.True),
          ("bar", Data.Arr(List(Data.Int(1), Data.Null, Data.Dec(2.3)))))).some
    }
  }
}
