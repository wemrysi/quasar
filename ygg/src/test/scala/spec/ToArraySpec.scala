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

import ygg._, common._

class ToArraySpec extends TableQspec {
  "in toArray" >> {
    "create a single column given two single columns" in testToArrayHomogeneous
    "create a single column given heterogeneous data" in testToArrayHeterogeneous
  }

  private def testToArrayHomogeneous = checkTableFun(
    fun = _.toArray[Double],
    data = jsonMany"""
      { "value": 23.4, "key": [ 1 ] }
      { "value": 12.4, "key": [ 2 ] }
      { "value": -12.4, "key": [ 3 ] }
    """,
    expected = jsonMany"[ 23.4 ] [ 12.4 ] [ -12.4 ]"
  )
  private def testToArrayHeterogeneous = checkTableFun(
    fun = _.toArray[Double],
    data = jsonMany"""
      {"key":[1],"value":{"bar":18.8,"foo":23.4}}
      {"key":[2],"value":{"bar":"a","foo":23.4}}
      {"key":[3],"value":{"bar":44.4}}
    """,
    expected = jsonMany"[ 18.8, 23.4 ]"
  )
}
