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
import qdata.time.TimeGenerators
import scalaz.syntax.std.option._

object QDataPreciseFacadeSpec extends Qspec {

  def parser[J: QDataEncode]: SupportParser[J] = new SupportParser[J] {
    implicit def facade: Facade[J] = QDataPreciseFacade.qdataPrecise[J]
  }

  // does not generate Data.NA
  def genAtomic: Gen[Data] =
    Gen.oneOf[Data](
      Data.Null,
      Data.True,
      Data.False,
      genUnicodeString ^^ Data.Str,
      DataGenerators.defaultInt ^^ Data.Int,
      DataGenerators.defaultDec ^^ Data.Dec,
      TimeGenerators.genInterval ^^ Data.Interval,
      TimeGenerators.genOffsetDateTime ^^ Data.OffsetDateTime,
      TimeGenerators.genOffsetDate ^^ Data.OffsetDate,
      TimeGenerators.genOffsetTime ^^ Data.OffsetTime,
      TimeGenerators.genLocalDateTime ^^ Data.LocalDateTime,
      TimeGenerators.genLocalDate ^^ Data.LocalDate,
      TimeGenerators.genLocalTime ^^ Data.LocalTime)

  implicit val codec: DataCodec = DataCodec.Precise

  implicit val arbData: Arbitrary[Data] =
    Arbitrary(DataGenerators.genDataDefault(genAtomic))

  "precise json facade parsing" >> {

    "parse arbitrary" >> prop { (data: Data) =>
      val json: Option[String] = DataCodec.render(data)
      json.flatMap(parser[Data].parseFromString(_).toOption) must_=== data.some
    }.set(minTestsOk = 1000)

    // a concrete example
    "parse concrete" >> {
      val json: String = """{ "foo": true, "bar": { "$localtime": "12:34" } }"""
      parser[Data].parseFromString(json).toOption must_===
        Data.Obj(ListMap(
          ("foo", Data.True),
          ("bar", Data.LocalTime(java.time.LocalTime.parse("12:34"))))).some
    }
  }
}
