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

import ygg._, common._, json._

object MergeDiffData {
  /** Diff **/
  def expectedChanges: JValue = json"""{
    "tags": ["static-typing","fp"],
    "features": { "key2": "newval2" }
  }"""

  def expectedAdditions: JValue = json"""{
    "features": { "key3": "val3"},
    "compiled": true
  }"""

  def expectedDeletions: JValue = json"""{
    "year":2006,
    "features":{ "key1":"val1" }
  }"""

  /** Common **/
  def scala1: JValue = json"""{
    "lang": "scala",
    "year": 2006,
    "tags": ["fp", "oo"],
    "features": {
      "key1":"val1",
      "key2":"val2"
    }
  }"""

  def scala2: JValue = json"""{
    "tags": ["static-typing","fp"],
    "compiled": true,
    "lang": "scala",
    "features": {
      "key2":"newval2",
      "key3":"val3"
    }
  }"""

  def expectedMergeResult: JValue = json"""{
    "lang": "scala",
    "year": 2006,
    "tags": ["fp", "oo", "static-typing"],
    "features": {
      "key1":"val1",
      "key2":"newval2",
      "key3":"val3"
    },
    "compiled": true
  }"""

  private def lotto(x: JValue): JValue = json"""{ "lotto": $x }"""

  def lotto1: JValue = lotto(json"""{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[{
      "winner-id":23,
      "numbers":[2,45,34,23,3,5]
    }]
  }""")

  def lotto2: JValue = lotto(json"""{
    "winners":[{
      "winner-id":54,
      "numbers":[52,3,12,11,18,22]
    }]
  }""")

  def mergedLottoResult: JValue = lotto(json"""{
    "lotto-id":5,
    "winning-numbers":[2,45,34,23,7,5,3],
    "winners":[{
      "winner-id":23,
      "numbers":[2,45,34,23,3,5]
    },{
      "winner-id":54,
      "numbers":[52,3,12,11,18,22]
    }]
  }""")
}
