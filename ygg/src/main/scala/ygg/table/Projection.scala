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
import scalaz._, Scalaz.{ ToIdOps => _, _ }

final case class BlockProjectionData[Key](minKey: Key, maxKey: Key, data: Slice)

final case class Projection(data: Stream[JValue]) {
  private val slices            = TableData.fromJValues(data).slices.toStream.copoint // XXX FiXME
  val length: Long              = data.length.toLong
  val structure: Set[ColumnRef] = slices.foldLeft(Set[ColumnRef]())(_ ++ _.columns.keySet)

  def getBlockStreamForType(tpe: JType): NeedSlices =
    getBlockStream(Some(Schema.flatten(tpe, structure.toVector).toSet))

  def getBlockStream(columns: Option[Set[ColumnRef]]): NeedSlices =
    unfoldStream(none[JArray])(key => getBlockAfter(key, columns) map (_ map (bpd => bpd.data -> some(bpd.maxKey))))

  /**
    * Get a block of data beginning with the first record with a key greater than
    * the specified key. If id.isEmpty, return a block starting with the minimum
    * key. Each resulting block should contain only the columns specified in the
    * column set; if the set of columns is empty, return all columns.
    */
  def getBlockAfter(id: Option[JArray], colSelection: Option[Set[ColumnRef]]) = Need {
    @tailrec def findBlockAfter(id: JArray, blocks: Stream[Slice]): Option[Slice] = {
      blocks.filterNot(_.isEmpty) match {
        case x #:: xs => if ((x.lastRow \ "key") > id) Some(x) else findBlockAfter(id, xs)
        case _        => None
      }
    }

    def isMatch(reqCols: Set[ColumnRef]): ColumnRef => Boolean = {
      case ColumnRef(CPath(CPathField("key"), _*), _) => true
      case ColumnRef(path, tpe)                       =>
        reqCols exists (r => (CPathField("value") \ r.selector) == path && r.ctype == tpe)
    }

    val slice: Option[Slice] = id.fold(slices.headOption)(findBlockAfter(_, slices))

    slice map (s =>
      Slice(s.size, colSelection.fold(s.columns)(req => s.columns filterKeys isMatch(req))) |> (s0 =>
        BlockProjectionData[JArray](s0.firstRow \ "key" asArray, s0.lastRow \ "key" asArray, s0)
      )
    )
  }
}
