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

package quasar.physical.marklogic.fs

import quasar.Data
import quasar.DataArbitrary._
import quasar.physical.marklogic.fs.data.{toXml, fromXml}
import quasar.physical.marklogic.xml.SecureXML

import scalaz._, Scalaz._

final class DataXmlEncodingSpec extends quasar.Qspec {
  type Result[A] = ErrorMessages \/ A

  "Data <-> XML encoding" should {
    "roundtrip" >> prop { data: Data =>
      toXml[Result](data).flatMap(fromXml[Result]) must_= data.right
    }

    "roundtrip through serialization" >> prop { data: Data =>
      val rt = for {
        xml <- toXml[Result](data)
        el  <- SecureXML.loadString(xml.toString)
                 .leftMap(e => e.toString.wrapNel)
        d   <- fromXml[Result](el)
      } yield d

      rt must_= data.right
    }
  }
}
