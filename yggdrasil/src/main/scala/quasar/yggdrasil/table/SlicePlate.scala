/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil
package table

import quasar.common.{CPath, CPathArray, CPathField, CPathIndex, CPathMeta}
import quasar.precog.common._

import tectonic.{Enclosure, Plate, Signal}

import scala.collection.mutable

private[table] final class SlicePlate
    extends Plate[List[Slice]]
    with ContinuingNestPlate[List[Slice]]
    with CPathPlate[List[Slice]] {

  private val MaxLongStrLength = Long.MinValue.toString.length

  private var nextThreshold = Config.defaultMinRows

  private var size = 0    // rows in process don't count toward size
  private val columns = mutable.Map[ColumnRef, ArrayColumn[_]]()

  private val completed = mutable.ListBuffer[Slice]()

  def nul(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CNull)).asInstanceOf[MutableNullColumn]
    col(size) = true

    Signal.Continue
  }

  def fls(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CBoolean)).asInstanceOf[ArrayBoolColumn]
    col(size) = false

    Signal.Continue
  }

  def tru(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CBoolean)).asInstanceOf[ArrayBoolColumn]
    col(size) = true

    Signal.Continue
  }

  def map(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CEmptyObject)).asInstanceOf[MutableEmptyObjectColumn]
    col(size) = true

    Signal.Continue
  }

  def arr(): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CEmptyArray)).asInstanceOf[MutableEmptyArrayColumn]
    col(size) = true

    Signal.Continue
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    growArrays()

    if (decIdx < 0 && expIdx < 0 && s.length < MaxLongStrLength) {
      val col = checkGet(ColumnRef(CPath(cursor.reverse), CLong)).asInstanceOf[ArrayLongColumn]
      col(size) = java.lang.Long.parseLong(s.toString)
    } else if (decIdx < 0) {
      try {
        val ln = java.lang.Long.parseLong(s.toString)
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CLong)).asInstanceOf[ArrayLongColumn]
        col(size) = ln
      } catch {
        case _: NumberFormatException =>
          val col = checkGet(ColumnRef(CPath(cursor.reverse), CNum)).asInstanceOf[ArrayNumColumn]
          col(size) = BigDecimal(s.toString)
      }
    } else {
      val num = BigDecimal(s.toString)

      if (num.isDecimalDouble) {
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CDouble)).asInstanceOf[ArrayDoubleColumn]
        col(size) = num.doubleValue
      } else {
        val col = checkGet(ColumnRef(CPath(cursor.reverse), CNum)).asInstanceOf[ArrayNumColumn]
        col(size) = num
      }
    }

    Signal.Continue
  }

  def str(s: CharSequence): Signal = {
    growArrays()

    val col = checkGet(ColumnRef(CPath(cursor.reverse), CString)).asInstanceOf[ArrayStrColumn]
    col(size) = s.toString

    Signal.Continue
  }

  def enclosure(): Enclosure = cursor match {
    case CPathIndex(_) :: _ => Enclosure.Array
    case CPathField(_) :: _ => Enclosure.Map
    case CPathMeta(_) :: _ => Enclosure.Meta
    case CPathArray :: _ => sys.error("no")
    case Nil => Enclosure.None
  }

  def finishRow(): Unit = {
    size += 1
    nextIndex = 0 :: Nil

    if (size > Config.maxSliceRows || columns.size > Config.maxSliceColumns) {
      finishSlice()
    }
  }

  def finishBatch(terminal: Boolean): List[Slice] = {
    if (terminal) {
      finishSlice()
    }

    val back = completed.toList
    completed.clear()

    back
  }

  private def checkGet(ref: ColumnRef): ArrayColumn[_] = {
    var back = columns.getOrElse(ref, null)

    if (back == null) {
      back = ref.ctype match {
        case CBoolean => ArrayBoolColumn.empty(nextThreshold)
        case CLong => ArrayLongColumn.empty(nextThreshold)
        case CDouble => ArrayDoubleColumn.empty(nextThreshold)
        case CNum => ArrayNumColumn.empty(nextThreshold)
        case CString => ArrayStrColumn.empty(nextThreshold)
        case CNull => MutableNullColumn.empty()
        case CEmptyArray => MutableEmptyArrayColumn.empty()
        case CEmptyObject => MutableEmptyObjectColumn.empty()
        case tpe => sys.error(s"should be impossible to create a column of $tpe")
      }

      columns(ref) = back
    }

    back
  }

  private def growArrays(): Unit = {
    if (size >= nextThreshold) {
      nextThreshold *= 2

      for ((ref, col) <- columns) {
        columns(ref) = col.resize(nextThreshold)
      }
    }
  }

  private def finishSlice(): Unit = {
    if (size > 0) {
      val slice = Slice(size, columns.to[λ[α => Map[ColumnRef, Column]]])
      completed += slice

      nextThreshold = Config.defaultMinRows
      size = 0
      columns.clear()
    }
  }
}
