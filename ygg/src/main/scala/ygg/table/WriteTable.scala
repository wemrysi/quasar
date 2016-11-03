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

import scalaz.{ Source => _, _ }, Scalaz._
import ygg._, common._
import JDBM._

object WriteTable {
  def apply[T <: ygg.table.Table](implicit companion: TableCompanion[T]) = new WriteTable[T]
}

class WriteTable[T <: ygg.table.Table](implicit companion: TableCompanion[T]) {
  def writeTables(slices: NeedSlices,
                  valueTrans: SliceTransform1[_],
                  keyTrans: Seq[SliceTransform1[_]],
                  sortOrder: DesiredSortOrder): Need[List[String] -> IndexMap] = {
    def write0(slices: NeedSlices, state: WriteState): Need[List[String] -> IndexMap] = {
      slices.uncons flatMap {
        case Some((slice, tail)) =>
          writeSlice(slice, state, sortOrder, "") flatMap { write0(tail, _) }

        case None =>
          Need {
            val closedJDBMState = state.jdbmState.closed()
            (state.keyTransformsWithIds map (_._2), closedJDBMState.indices)
          }
      }
    }
    val identifiedKeyTrans = keyTrans.zipWithIndex map { case (kt, i) => kt -> i.toString }
    write0(companion.reduceSlices(slices), WriteState(JDBMState.empty("writeSortedSpace"), valueTrans, identifiedKeyTrans.toList))
  }

  def writeSlice(slice: Slice, state: WriteState, sortOrder: DesiredSortOrder, source: String): Need[WriteState] = {
    val WriteState(jdbmState, valueTrans, keyTrans) = state

    valueTrans.advance(slice) flatMap {
      case (valueTrans0, vslice) => {
        val kvs               = vslice.columns.toList.sortBy(_._1)
        val vColumnRefs       = kvs map (_._1)
        val vColumns          = kvs map (_._2)
        val dataRowFormat     = RowFormat.forValues(vColumnRefs)
        val dataColumnEncoder = dataRowFormat.ColumnEncoder(vColumns)

        def storeTransformed(jdbmState: JDBMState,
                             transforms: List[SliceTransform1[_] -> String],
                             updatedTransforms: List[SliceTransform1[_] -> String]): Need[JDBMState -> List[SliceTransform1[_] -> String]] = transforms match {
          case (keyTransform, streamId) :: tail =>
            keyTransform.advance(slice) flatMap {
              case (nextKeyTransform, kslice) =>
                def thing = (nextKeyTransform -> streamId) :: updatedTransforms

                kslice.columns.toList.sortBy(_._1) match {
                  case Seq() => Need(jdbmState -> thing)
                  case _     => writeRawSlices(kslice, sortOrder, vslice, vColumnRefs, dataColumnEncoder, streamId, jdbmState) flatMap (storeTransformed(_, tail, thing))
                }
            }

          case Nil =>
            Need((jdbmState, updatedTransforms.reverse))
        }

        storeTransformed(jdbmState, keyTrans, Nil) map {
          case (jdbmState0, keyTrans0) =>
            WriteState(jdbmState0, valueTrans0, keyTrans0)
        }
      }
    }
  }

  def writeAlignedSlices(kslice: Slice, vslice: Slice, jdbmState: JDBMState, indexNamePrefix: String, sortOrder: DesiredSortOrder) = {
    val kvs         = vslice.columns.toList.sortBy(_._1)
    val vColumnRefs = kvs map (_._1)
    val vColumns    = kvs map (_._2)

    val dataRowFormat     = RowFormat.forValues(vColumnRefs)
    val dataColumnEncoder = dataRowFormat.ColumnEncoder(vColumns)

    writeRawSlices(kslice, sortOrder, vslice, vColumnRefs, dataColumnEncoder, indexNamePrefix, jdbmState)
  }

  protected def writeRawSlices(kslice: Slice,
                               sortOrder: DesiredSortOrder,
                               vslice: Slice,
                               vrefs: List[ColumnRef],
                               vEncoder: ColumnEncoder,
                               indexNamePrefix: String,
                               jdbmState: JDBMState): Need[JDBMState] = Need {
    // Iterate over the slice, storing each row
    // FIXME: Determine whether undefined sort keys are valid
    def storeRows(kslice: Slice, vslice: Slice, keyRowFormat: RowFormat, vEncoder: ColumnEncoder, storage: IndexStore, insertCount: Long): Long = {

      val keyColumns = kslice.columns.toList.sortBy(_._1).map(_._2)
      val kEncoder   = keyRowFormat.ColumnEncoder(keyColumns)

      @tailrec def storeRow(row: Int, insertCount: Long): Long = {
        if (row < vslice.size) {
          if (vslice.isDefinedAt(row) && kslice.isDefinedAt(row)) {
            storage.put(kEncoder.encodeFromRow(row), vEncoder.encodeFromRow(row))

            if (insertCount % jdbmCommitInterval == 0 && insertCount > 0) jdbmState.commit()
            storeRow(row + 1, insertCount + 1)
          } else {
            storeRow(row + 1, insertCount)
          }
        } else {
          insertCount
        }
      }

      storeRow(0, insertCount)
    }

    val krefs       = kslice.columns.keys.toList.sorted
    val indexMapKey = IndexKey(indexNamePrefix, krefs, vrefs)

    // There are 3 cases:
    //  1) No entry in the indices: We sort the slice and add a SortedSlice entry.
    //  2) A SortedSlice entry in the index: We store it and the current slice in
    //     a JDBM Index and replace the entry with a SliceIndex.
    //  3) A SliceIndex entry in the index: We add the current slice in the JDBM
    //     index and update the SliceIndex entry.

    jdbmState.indices.get(indexMapKey) map {
      case sliceIndex: JDBM.SliceIndex =>
        (sliceIndex, jdbmState)

      case SortedSlice(indexName, kslice0, vslice0, vEncoder0, keyRefs, valRefs, count) =>
        val keyRowFormat                  = RowFormat.forSortingKey(krefs)
        val keyComparator                 = SortingKeyComparator(keyRowFormat, sortOrder.isAscending)
        val (dbFile, db, openedJdbmState) = jdbmState.opened()
        val storage                       = db.hashMap(indexName, keyComparator, ByteArraySerializer).create()
        val count                         = storeRows(kslice0, vslice0, keyRowFormat, vEncoder0, storage, 0)
        val sliceIndex                    = JDBM.SliceIndex(indexName, dbFile, storage, keyRowFormat, keyComparator, keyRefs, valRefs, count)

        (sliceIndex, openedJdbmState.copy(indices = openedJdbmState.indices + (indexMapKey -> sliceIndex), insertCount = count))

    } map {
      case (index, jdbmState) =>
        val newInsertCount = storeRows(kslice, vslice, index.keyRowFormat, vEncoder, index.storage, jdbmState.insertCount)

        // Although we have a global count of inserts, we also want to
        // specifically track counts on the index since some operations
        // may not use all indices (e.g. groupByN)
        val newIndex = index.copy(count = index.count + (newInsertCount - jdbmState.insertCount))

        jdbmState.copy(indices = jdbmState.indices + (indexMapKey -> newIndex), insertCount = newInsertCount)

    } getOrElse {
      // sort k/vslice and shove into SortedSlice.
      val indexName = indexMapKey.name
      val mvslice   = vslice.materialized
      val mkslice   = kslice.materialized

      // TODO Materializing after a sort may help w/ cache hits when traversing a column.
      val (vslice0, kslice0) = mvslice.sortWith(mkslice, sortOrder)
      val sortedSlice        = SortedSlice(indexName, kslice0, vslice0, vEncoder, krefs.toArray, vrefs.toArray, vslice0.size.toLong)

      jdbmState.copy(indices = jdbmState.indices + (indexMapKey -> sortedSlice), insertCount = 0)
    }
  }

  import trans._

  def writeSorted(table: T, keys: Seq[TransSpec1], spec: TransSpec1, sort: DesiredSortOrder, uniq: Boolean): Need[Seq[String] -> IndexMap] = uniq match {
    case false => writeSortedNonUnique(table, keys, spec, sort)
    case true  => writeSortedUnique(table, keys, spec, sort)
  }

  def writeSortedUnique(table: T, groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, order: DesiredSortOrder): Need[Seq[String] -> IndexMap] =
    writeTables(
      table transform root.spec slices,
      composeSliceTransform(valueSpec),
      groupKeys map composeSliceTransform,
      order
    )

  def writeSortedNonUnique(table: T, groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, order: DesiredSortOrder): Need[Seq[String] -> IndexMap] = {
    val keys1 = groupKeys map (kt => OuterObjectConcat(WrapObject(kt deepMap { case Leaf(_) => root(0) } spec, "0"), WrapObject(root(1), "1")))
    writeTables(
      table transform companion.addGlobalId(root.spec) slices,
      composeSliceTransform(valueSpec deepMap { case Leaf(_) => TransSpec1.DerefArray0 } spec),
      keys1 map composeSliceTransform,
      order
    )
  }
}
