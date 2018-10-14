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

package quasar.mimir.evaluate

import quasar.ParseInstructionSpec

import tectonic.AsyncParser
import tectonic.test.{Event, ReifiedTerminalPlate}

object TectonicParseInstructionSpec extends ParseInstructionSpec {
  type JsonStream = List[Event]

  def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream = {
    val plate = new PivotPlate(pivot, new ReifiedTerminalPlate)
    ReifiedTerminalPlate.visit(stream, plate)
  }

  protected def ldjson(str: String): JsonStream = {
    val plate = new ReifiedTerminalPlate
    val parser = AsyncParser(plate, AsyncParser.ValueStream)

    val events1 = parser.absorb(str).right.get
    val events2 = parser.finish().right.get

    events1 ::: events2
  }
}
