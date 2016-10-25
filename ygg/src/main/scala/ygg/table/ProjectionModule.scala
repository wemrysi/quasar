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

package ygg.table

import ygg._, common._
import scalaz._, Scalaz._

final case class BlockProjectionData[Key](minKey: Key, maxKey: Key, data: Slice)

trait ProjectionLike {
  type Key
  def structure: Need[Set[ColumnRef]]
  def length: Long

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]]): Need[Option[BlockProjectionData[Key]]]

  def getBlockStream(columns: Option[Set[ColumnRef]]): NeedSlices = unfoldStream(none[Key])(key =>
    getBlockAfter(key, columns) map (
      _ collect { case BlockProjectionData(_, max, block) => block -> some(max) }
    )
  )
}
