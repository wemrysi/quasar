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

import scalaz._, Scalaz._
import ygg._, common._, json._, table._

class PartitionMergeSpec extends TableQspec {
  import trans._

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }

  private object reducer extends CReducer[String] {
    def reduce(schema: CSchema, range: Range): String = {
      schema.columns(JTextT).head match {
        case col: StrColumn => range map col mkString ";"
        case _              => abort("Not a StrColumn")
      }
    }
  }

  def testPartitionMerge = {
    val data = jsonMany"""
      { "key": [0], "value": { "a": "0a" } }
      { "key": [1], "value": { "a": "1a" } }
      { "key": [1], "value": { "a": "1b" } }
      { "key": [1], "value": { "a": "1c" } }
      { "key": [2], "value": { "a": "2a" } }
      { "key": [3], "value": { "a": "3a" } }
      { "key": [3], "value": { "a": "3b" } }
      { "key": [4], "value": { "a": "4a" } }
    """
    val expected = jsonMany"""
      "0a"
      "1a;1b;1c"
      "2a"
      "3a;3b"
      "4a"
    """
    val tbl    = fromJson(data)
    val result = tbl.partitionMerge(dotKey)(_ transform dotValue.a reduce reducer map (Table constString _))

    result.flatMap(_.toJson).copoint must_=== expected.toStream
  }
}
