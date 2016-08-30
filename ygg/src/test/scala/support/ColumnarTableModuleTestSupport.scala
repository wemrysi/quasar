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

import scalaz._
import scalaz.syntax.std.boolean._
import ygg._, common._, data._, json._, table._

trait ColumnarTableModuleTestSupport extends ColumnarTableModule with TableModuleTestSupport {
  private val idGen       = new AtomicIntIdSource(new GroupId(_))
  def newGroupId: GroupId = idGen.nextId()

  private def makeSlice(sampleData: Stream[JValue], sliceSize: Int): (Slice, Stream[JValue]) = {
    @tailrec def buildColArrays(from: Stream[JValue], into: Map[ColumnRef, ArrayColumn[_]], sliceIndex: Int): (Map[ColumnRef, ArrayColumn[_]], Int) = {
      from match {
        case jv #:: xs =>
          val refs = Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (prefix, suffix) = sampleData.splitAt(sliceSize)
    val slice            = Slice(buildColArrays(prefix.toStream, Map.empty[ColumnRef, ArrayColumn[_]], 0))

    (slice, suffix)
  }

  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], sliceSize: Int): Table = {
    Table(
      StreamT.unfoldM(values) { events =>
        Need {
          (!events.isEmpty) option {
            makeSlice(events.toStream, sliceSize)
          }
        }
      },
      ExactSize(values.length)
    )
  }

  def fromJson(values: Seq[JValue], maxSliceSize: Option[Int]): Table =
    fromJson0(values.toStream, maxSliceSize getOrElse yggConfig.maxSliceSize)
}
