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

package quasar

import quasar.common.CPath

import org.specs2.matcher.Matcher

abstract class ParseInstructionSpec
    extends JsonSpec
    with ParseInstructionSpec.IdsSpec
    with ParseInstructionSpec.WrapSpec
    with ParseInstructionSpec.MasksSpec
    with ParseInstructionSpec.PivotSpec

object ParseInstructionSpec {
  // TODO these things too
  trait IdsSpec extends JsonSpec
  trait WrapSpec extends JsonSpec
  trait MasksSpec extends JsonSpec

  trait PivotSpec extends JsonSpec {
    protected final type Pivot = ParseInstruction.Pivot
    protected final val Pivot = ParseInstruction.Pivot

    "pivot" should {
      "shift an array at identity" >> {
        val input = ldjson("""
          [1, 2, 3]
          [4, 5, 6]
          [7, 8, 9, 10]
          [11]
          []
          [12, 13]
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            1
            2
            3
            4
            5
            6
            7
            8
            9
            10
            11
            12
            13
            """)

          input must pivotInto(CPath.parse("."), IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            0
            1
            2
            0
            1
            2
            0
            1
            2
            3
            0
            0
            1
            """)

          input must pivotInto(CPath.parse("."), IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            [0, 1]
            [1, 2]
            [2, 3]
            [0, 4]
            [1, 5]
            [2, 6]
            [0, 7]
            [1, 8]
            [2, 9]
            [3, 10]
            [0, 11]
            [0, 12]
            [1, 13]
            """)

          input must pivotInto(CPath.parse("."), IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }

      "shift an array at .a.b (with no surrounding structure)" >> {
        val input = ldjson("""
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": [4, 5, 6] } }
          { "a": { "b": [7, 8, 9, 10] } }
          { "a": { "b": [11] } }
          { "a": { "b": [] } }
          { "a": { "b": [12, 13] } }
          """)

        "ExcludeId" >> {
          val expected = ldjson("""
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 3 } }
            { "a": { "b": 4 } }
            { "a": { "b": 5 } }
            { "a": { "b": 6 } }
            { "a": { "b": 7 } }
            { "a": { "b": 8 } }
            { "a": { "b": 9 } }
            { "a": { "b": 10 } }
            { "a": { "b": 11 } }
            { "a": { "b": 12 } }
            { "a": { "b": 13 } }
            """)

          input must pivotInto(CPath.parse(".a.b"), IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            { "a": { "b": 2 } }
            { "a": { "b": 3 } }
            { "a": { "b": 0 } }
            { "a": { "b": 0 } }
            { "a": { "b": 1 } }
            """)

          input must pivotInto(CPath.parse(".a.b"), IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": { "b": [0, 1] } }
            { "a": { "b": [1, 2] } }
            { "a": { "b": [2, 3] } }
            { "a": { "b": [0, 4] } }
            { "a": { "b": [1, 5] } }
            { "a": { "b": [2, 6] } }
            { "a": { "b": [0, 7] } }
            { "a": { "b": [1, 8] } }
            { "a": { "b": [2, 9] } }
            { "a": { "b": [3, 10] } }
            { "a": { "b": [0, 11] } }
            { "a": { "b": [0, 12] } }
            { "a": { "b": [1, 13] } }
            """)

          input must pivotInto(CPath.parse(".a.b"), IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }
    }

    def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream

    def pivotInto(
        path: CPath,
        idStatus: IdStatus,
        structure: CompositeParseType)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalPivot(Pivot(path, idStatus, structure), str)}
  }
}
