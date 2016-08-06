/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.yggdrasil

import quasar.precog._
import blueeyes._
import com.precog.common._
import com.precog.yggdrasil.table._
import JDBM._

object JDBM {
  type Bytes             = Array[Byte]
  type BtoBEntry         = jMapEntry[Bytes, Bytes]
  type BtoBIterator      = Iterator[BtoBEntry]
  type BtoBMap           = java.util.SortedMap[Bytes, Bytes]
  type BtoBConcurrentMap = jConcurrentMap[Bytes, Bytes]

  final case class JSlice(firstKey: Bytes, lastKey: Bytes, rows: Int)
}

object JDBMSlice {
  def load(size: Int, source: () => BtoBIterator, keyDecoder: ColumnDecoder, valDecoder: ColumnDecoder): JSlice = {
    var firstKey: Bytes = null
    var lastKey: Bytes  = null

    @tailrec
    def consumeRows(source: BtoBIterator, row: Int): Int = (
      if (!source.hasNext) row else {
        val entry  = source.next
        val rowKey = entry.getKey
        if (row == 0)
          firstKey = rowKey

        lastKey = rowKey

        keyDecoder.decodeToRow(row, rowKey)
        valDecoder.decodeToRow(row, entry.getValue)
        consumeRows(source, row + 1)
      }
    )

    // FIXME: Looping here is a blatantly poor way to work around ConcurrentModificationExceptions
    // From the Javadoc for CME, the exception is an indication of a bug
    val rows: Int = {
      var finalCount: Try[Int] = ScalaFailure(new Exception)
      var tries                = JDBMProjection.MAX_SPINS
      while (tries > 0 && finalCount.isFailure) {
        finalCount = Try(consumeRows(source().take(size), 0))
        if (finalCount.isFailure) {
          tries -= 1
          Thread sleep 50
        }
      }
      finalCount getOrElse ( throw new VicciniException("Block read failed with too many concurrent mods.") )
    }

    JSlice(firstKey, lastKey, rows)
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
