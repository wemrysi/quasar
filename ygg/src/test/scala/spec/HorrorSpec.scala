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

import ygg._, common._, table._, trans._, json._
import scalaz._

/** SQL examples from
 *    https://blog.codinghorror.com/a-visual-explanation-of-sql-joins/
 */
class HorrorSpec extends TableQspec {
  val TableA = fromJson(
    jsonMany"""
      { "id": 1, "name": "Pirate" }
      { "id": 2, "name": "Monkey" }
      { "id": 3, "name": "Ninja" }
      { "id": 4, "name": "Spaghetti" }
    """
  )
  val TableB = fromJson(
    jsonMany"""
      { "id": 1, "name": "Rutabaga" }
      { "id": 2, "name": "Pirate" }
      { "id": 3, "name": "Darth Vader" }
      { "id": 4, "name": "Ninja" }
    """
  )
  val TableAB = TablePair(TableA -> TableB)

  def mkSource(table: TableData, groupId: Int, cond: GroupKeySpecSource) = GroupingSource(
    table,
    'name,
    Some(ID),
    groupId,
    cond
  )

  def alignment = GroupingAlignment.intersect(
    mkSource(TableA, 1, GroupKeySpecSource("tic_a", ID)),
    mkSource(TableB, 2, GroupKeySpecSource("tic_b", ID))
  )

  "in coding horror example" >> {
    "inner join" >> {
      def expected = jsonMany"""
        [ 1, 2 ]
        [ 3, 4 ]
      """
      def result = Table.merge(alignment) { (key, map) =>
        (key \ "tic_a" \ "name", key \ "tic_b" \ "name") match {
          case (CString(x), CString(y)) if x == y =>
            val i1: JValue = key \ "tic_a" \ "id" toJValue
            val i2: JValue = key \ "tic_b" \ "id" toJValue;

            Need(fromJson(jsonMany"[ $i1, $i2 ]"))
          case _ =>
            Need(Table.empty)
        }
      }
      result.value.toSeq.sorted must_=== expected
    }
  }
}

