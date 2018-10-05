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
package table

import quasar.blueeyes.json.JValue
import quasar.precog.JsonTestSupport

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import tectonic.AsyncParser

object SlicePlateSpec extends Specification with ScalaCheck {
  import JsonTestSupport._

  "slice plate parsing through tectonic" should {
    "round trip" in prop { values: List[JValue] =>
      val input = values.mkString("\n")
      val plate = new SlicePlate
      val parser = AsyncParser(plate, AsyncParser.ValueStream)

      (parser.absorb(input), parser.finish()) must beLike {
        case (Right(slices1), Right(slices2)) =>
          val results = (slices1 ++ slices2).flatMap(_.toJsonElements)
          results mustEqual values
      }
    }
  }
}
