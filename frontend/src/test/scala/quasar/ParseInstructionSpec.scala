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

// the implicits aren't part of qspec? wtf?
import org.specs2.matcher.{Matcher, MatchersImplicits}

import scala.StringContext

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

  trait PivotSpec extends JsonSpec with MatchersImplicits {
    protected final type Pivot = ParseInstruction.Pivot
    protected final val Pivot = ParseInstruction.Pivot

    "pivot" should {
      "shift an array at identity" >> {
        val input = ldjson"""
          [1, 2, 3]
          [4, 5, 6]
          [7, 8, 9, 10]
          [11]
          []
          [12, 13]
          """

        "ExcludeId" >> {
          val expected = ldjson"""
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
            """

          input must pivotInto(CPath.parse("."), IdStatus.ExcludeId, ParseType.Array)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson"""
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
            """

          input must pivotInto(CPath.parse("."), IdStatus.IdOnly, ParseType.Array)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson"""
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
            """

          input must pivotInto(CPath.parse("."), IdStatus.IncludeId, ParseType.Array)(expected)
        }
      }
    }

    def evalPivot(pivot: Pivot, stream: JsonStream): JsonStream

    def pivotInto(
        path: CPath,
        idStatus: IdStatus,
        structure: CompositeParseType)(
        expected: JsonStream)
        : Matcher[JsonStream] = { input: JsonStream =>
      evalPivot(Pivot(path, idStatus, structure), input) === expected
    }
  }
}
