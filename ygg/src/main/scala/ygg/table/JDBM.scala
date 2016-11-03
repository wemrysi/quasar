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
import org.mapdb._
import JDBM._

final case class JDBMState(prefix: String, fdb: Option[jFile -> DB], indices: IndexMap, insertCount: Long) {
  def commit() = fdb foreach { _._2.commit() }

  def closed(): JDBMState = fdb match {
    case Some((f, db)) =>
      db.close()
      JDBMState(prefix, None, indices, insertCount)
    case None => this
  }

  def opened(): (jFile, DB, JDBMState) = fdb match {
    case Some((f, db)) => (f, db, this)
    case None          =>
      // Open a JDBM3 DB for use in sorting under a temp directory
      val dbFile = new jFile(newScratchDir(), prefix)
      val db     = DBMaker.fileDB(dbFile.getCanonicalPath).make()
      (dbFile, db, JDBMState(prefix, Some((dbFile, db)), indices, insertCount))
  }
}
object JDBMState {
  def empty(prefix: String) = JDBMState(prefix, None, Map(), 0l)
}

object JDBM {
  def jdbmCommitInterval: Long = 200000l

  type BtoBEntry         = jMapEntry[Bytes, Bytes]
  type BtoBIterator      = scIterator[BtoBEntry]
  type BtoBMap           = java.util.SortedMap[Bytes, Bytes]
  type BtoBConcurrentMap = jConcurrentMap[Bytes, Bytes]
  type IndexStore        = BtoBConcurrentMap
  type IndexMap          = Map[IndexKey, SliceSorter]

  final case class JSlice(firstKey: Bytes, lastKey: Bytes, rows: Int)

  sealed trait SliceSorter {
    def name: String
    def keyRefs: Array[ColumnRef]
    def valRefs: Array[ColumnRef]
    def count: Long
  }

  final case class SliceIndex(name: String,
                        dbFile: jFile,
                        storage: IndexStore,
                        keyRowFormat: RowFormat,
                        keyComparator: Comparator[Bytes],
                        keyRefs: Array[ColumnRef],
                        valRefs: Array[ColumnRef],
                        count: Long) extends SliceSorter

  final case class SortedSlice(name: String,
                         kslice: Slice,
                         vslice: Slice,
                         valEncoder: ColumnEncoder,
                         keyRefs: Array[ColumnRef],
                         valRefs: Array[ColumnRef],
                         count: Long) extends SliceSorter

  final case class IndexKey(streamId: String, keyRefs: List[ColumnRef], valRefs: List[ColumnRef]) {
    val name = streamId + ";krefs=" + keyRefs.mkString("[", ",", "]") + ";vrefs=" + valRefs.mkString("[", ",", "]")
  }

  def columnFor(prefix: CPath, sliceSize: Int)(ref: ColumnRef): ColumnRef -> ArrayColumn[_] = ((
     ref.copy(selector = prefix \ ref.selector),
     ref.ctype match {
       case CString              => ArrayStrColumn.empty(sliceSize)
       case CBoolean             => ArrayBoolColumn.empty()
       case CLong                => ArrayLongColumn.empty(sliceSize)
       case CDouble              => ArrayDoubleColumn.empty(sliceSize)
       case CNum                 => ArrayNumColumn.empty(sliceSize)
       case CDate                => ArrayDateColumn.empty(sliceSize)
       case CPeriod              => ArrayPeriodColumn.empty(sliceSize)
       case CNull                => MutableNullColumn.empty()
       case CEmptyObject         => MutableEmptyObjectColumn.empty()
       case CEmptyArray          => MutableEmptyArrayColumn.empty()
       case CArrayType(elemType) => ArrayHomogeneousArrayColumn.empty(sliceSize)(elemType)
       case CUndefined           => abort("CUndefined cannot be serialized")
     }
  ))
}
