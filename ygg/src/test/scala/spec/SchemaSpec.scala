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

package ygg.tests

import ygg._, common._, json._, table._

class SchemaSpec extends quasar.Qspec {
  "cpath" should {
    "return the correct sequence of CPath" in {
      val jtype = JType.Object(
        "foo" -> JNumberT,
        "bar" -> JType.Array(
          JBooleanT,
          JType.Object("baz" -> JArrayHomogeneousT(JNullT)),
          JTextT
        )
      )
      val expected = Vector[CPath](
        "foo",
        "bar[0]",
        "bar[1].baz[*]",
        "bar[2]"
      )

      jtype.cpaths must_=== expected.sorted
    }
  }
}
