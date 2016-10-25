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

import ygg._, common._, json._
import scalaz._, Scalaz._

final case class BlockProjectionData[Key](minKey: Key, maxKey: Key, data: Slice)

final case class Projection(data: Stream[JValue]) {
  type Key = JArray

  private val slices            = Table.fromJson(data).slices.toStream.copoint
  val length: Long              = data.length.toLong
  val structure: Set[ColumnRef] = slices.foldLeft(Set[ColumnRef]())(_ ++ _.columns.keySet)

  def getBlockStreamForType(tpe: JType): NeedSlices =
    getBlockStream(Some(Schema.flatten(tpe, structure.toVector).toSet))

  def getBlockStream(columns: Option[Set[ColumnRef]]): NeedSlices = unfoldStream(none[Key])(key =>
    getBlockAfter(key, columns) map (
      _ collect { case BlockProjectionData(_, max, block) => block -> some(max) }
    )
  )

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[JArray], colSelection: Option[Set[ColumnRef]]) = Need {
    @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
      blocks.filterNot(_.isEmpty) match {
        case x #:: xs => if ((x.toJson(x.size - 1).getOrElse(JUndefined) \ "key") > id) Some(x) else findBlockAfter(id, xs)
        case _        => None
      }
    }

    val slice = id map (findBlockAfter(_, slices)) getOrElse slices.headOption

    slice map { s =>
      val s0 = Slice(s.size, {
        colSelection.map { reqCols =>
          s.columns.filter {
            case (ref @ ColumnRef(jpath, ctype), _) =>
              jpath.nodes.head == CPathField("key") || reqCols.exists { ref =>
                (CPathField("value") \ ref.selector) == jpath && ref.ctype == ctype
              }
          }
        }.getOrElse(s.columns)
      })

      BlockProjectionData[JArray](
        s0.toJson(0).getOrElse(JUndefined) \ "key" asArray,
        s0.toJson(s0.size - 1).getOrElse(JUndefined) \ "key" asArray,
        s0)
    }
  }
}
