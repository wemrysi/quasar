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

import java.lang.String

abstract class ParseInstructionSpec
    extends JsonSpec
    with ParseInstructionSpec.IdsSpec
    with ParseInstructionSpec.WrapSpec
    with ParseInstructionSpec.MasksSpec
    with ParseInstructionSpec.PivotSpec

object ParseInstructionSpec {
  // TODO these things too
  trait IdsSpec extends JsonSpec

  trait WrapSpec extends JsonSpec {
    protected final type Wrap = ParseInstruction.Wrap
    protected final val Wrap = ParseInstruction.Wrap

    "wrap" should {
      "nest scalars at ." in {
        val input = ldjson("""
          1
          "hi"
          true
          """)

        val expected = ldjson("""
          { "foo": 1 }
          { "foo": "hi" }
          { "foo": true }
          """)

        input must wrapInto(CPath.parse("."), "foo")(expected)
      }

      "nest vectors at ." in {
        val input = ldjson("""
          [1, 2, 3]
          { "a": "hi", "b": { "c": null } }
          [{ "d": {} }]
          """)

        val expected = ldjson("""
          { "bar": [1, 2, 3] }
          { "bar": { "a": "hi", "b": { "c": null } } }
          { "bar": [{ "d": {} }] }
          """)

        input must wrapInto(CPath.parse("."), "bar")(expected)
      }

      "nest scalars at .a.b" in {
        val input = ldjson("""
          { "a": { "b": 1 } }
          { "a": { "b": "hi" } }
          { "a": { "b": true } }
          """)

        val expected = ldjson("""
          { "a": { "b": { "foo": 1 } } }
          { "a": { "b": { "foo": "hi" } } }
          { "a": { "b": { "foo": true } } }
          """)

        input must wrapInto(CPath.parse(".a.b"), "foo")(expected)
      }

      "nest scalars at .a[1] with surrounding structure" in {
        val input = ldjson("""
          { "a": [null, 1, "nada"] }
          { "a": [[], "hi"] }
          { "a": [null, true, "nada"] }
          """)

        val expected = ldjson("""
          { "a": [null, { "bin": 1 }, "nada"] }
          { "a": [[], { "bin": "hi" }] }
          { "a": [null, { "bin": true }, "nada"] }
          """)

        input must wrapInto(CPath.parse(".a[1]"), "bin")(expected)
      }

      "nest vectors at .a.b" in {
        val input = ldjson("""
          { "a": { "b": [1, 2, 3] } }
          { "a": { "b": { "a": "hi", "b": { "c": null } } } }
          { "a": { "b": [{ "d": {} }] } }
          """)

        val expected = ldjson("""
          { "a": { "b": { "bar": [1, 2, 3] } } }
          { "a": { "b": { "bar": { "a": "hi", "b": { "c": null } } } } }
          { "a": { "b": { "bar": [{ "d": {} }] } } }
          """)

        input must wrapInto(CPath.parse(".a.b"), "bar")(expected)
      }

      "retain surrounding structure with scalars at .a.b" in {
        val input = ldjson("""
          { "z": null, "a": { "b": 1 }, "d": false, "b": [] }
          { "z": [], "a": { "b": "hi" }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": true }, "d": {} }
          """)

        val expected = ldjson("""
          { "z": null, "a": { "b": { "qux": 1 } }, "d": false, "b": [] }
          { "z": [], "a": { "b": { "qux": "hi" } }, "d": "to be or not to be", "b": { "a": 321} }
          { "z": { "a": { "b": 3.14 } }, "a": { "b": { "qux": true } }, "d": {} }
          """)

        input must wrapInto(CPath.parse(".a.b"), "foo")(expected)
      }
    }

    def evalWrap(pivot: Wrap, stream: JsonStream): JsonStream

    def wrapInto(
        path: CPath,
        name: String)(
        expected: JsonStream)
        : Matcher[JsonStream] =
      bestSemanticEqual(expected) ^^ { str: JsonStream => evalWrap(Wrap(path, name), str)}
  }

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

      "shift an object at identity" >> {
        val input = ldjson("""
          { "a": 1, "b": 2, "c": 3 }
          { "d": 4, "e": 5, "f": 6 }
          { "g": 7, "h": 8, "i": 9, "j": 10 }
          { "k": 11 }
          {}
          { "l": 12, "m": 13 }
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

          input must pivotInto(CPath.parse("."), IdStatus.ExcludeId, ParseType.Object)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            "a"
            "b"
            "c"
            "d"
            "e"
            "f"
            "g"
            "h"
            "i"
            "j"
            "k"
            "l"
            "m"
            """)

          input must pivotInto(CPath.parse("."), IdStatus.IdOnly, ParseType.Object)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            ["a", 1]
            ["b", 2]
            ["c", 3]
            ["d", 4]
            ["e", 5]
            ["f", 6]
            ["g", 7]
            ["h", 8]
            ["i", 9]
            ["j", 10]
            ["k", 11]
            ["l", 12]
            ["m", 13]
            """)

          input must pivotInto(CPath.parse("."), IdStatus.IncludeId, ParseType.Object)(expected)
        }
      }

      "shift an object at .a.b (no surrounding structure)" >> {
        val input = ldjson("""
          { "a": { "b": { "a": 1, "b": 2, "c": 3 } } }
          { "a": { "b": { "d": 4, "e": 5, "f": 6 } } }
          { "a": { "b": { "g": 7, "h": 8, "i": 9, "j": 10 } } }
          { "a": { "b": { "k": 11 } } }
          { "a": { "b": {} } }
          { "a": { "b": { "l": 12, "m": 13 } } }
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

          input must pivotInto(CPath.parse(".a.b"), IdStatus.ExcludeId, ParseType.Object)(expected)
        }

        "IdOnly" >> {
          val expected = ldjson("""
            { "a": { "b": "a" } }
            { "a": { "b": "b" } }
            { "a": { "b": "c" } }
            { "a": { "b": "d" } }
            { "a": { "b": "e" } }
            { "a": { "b": "f" } }
            { "a": { "b": "g" } }
            { "a": { "b": "h" } }
            { "a": { "b": "i" } }
            { "a": { "b": "j" } }
            { "a": { "b": "k" } }
            { "a": { "b": "l" } }
            { "a": { "b": "m" } }
            """)

          input must pivotInto(CPath.parse(".a.b"), IdStatus.IdOnly, ParseType.Object)(expected)
        }

        "IncludeId" >> {
          val expected = ldjson("""
            { "a": { "b": ["a", 1] } }
            { "a": { "b": ["b", 2] } }
            { "a": { "b": ["c", 3] } }
            { "a": { "b": ["d", 4] } }
            { "a": { "b": ["e", 5] } }
            { "a": { "b": ["f", 6] } }
            { "a": { "b": ["g", 7] } }
            { "a": { "b": ["h", 8] } }
            { "a": { "b": ["i", 9] } }
            { "a": { "b": ["j", 10] } }
            { "a": { "b": ["k", 11] } }
            { "a": { "b": ["l", 12] } }
            { "a": { "b": ["m", 13] } }
            """)

          input must pivotInto(CPath.parse(".a.b"), IdStatus.IncludeId, ParseType.Object)(expected)
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
