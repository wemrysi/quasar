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

package quasar.yggdrasil.nihdb

import org.slf4j.LoggerFactory

import quasar.precog.BitSet
import quasar.precog.common._
import quasar.yggdrasil._
import quasar.yggdrasil.table._
import quasar.niflheim._
import qdata.time.{DateTimeInterval, OffsetDate}

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}

case class SegmentsWrapper(segments: Seq[Segment], projectionId: Int, blockId: Long) extends Slice {
  import TransSpecModule.paths

  val logger = LoggerFactory.getLogger("com.precog.yggdrasil.table.SegmentsWrapper")

  // FIXME: This should use an identity of Array[Long](projectionId,
  // blockId), but the evaluator will cry if we do that right now
  private def keyFor(row: Int): Long = {
    (projectionId.toLong << 44) ^ (blockId << 16) ^ row.toLong
  }

  private def buildKeyColumn(length: Int): (ColumnRef, Column) = {
    val keys = new Array[Long](length)
    var i = 0
    while (i < length) {
      keys(i) = keyFor(i)
      i += 1
    }
    (ColumnRef(CPath(paths.Key) \ 0, CLong), ArrayLongColumn(keys))
  }

  private def buildColumnRef(seg: Segment) = ColumnRef(CPath(paths.Value) \ seg.cpath, seg.ctype)

  private def buildColumn(seg: Segment): Column = seg match {
    case segment: ArraySegment[a] =>
      val ctype: CValueType[a] = segment.ctype
      val defined: BitSet = segment.defined
      val values: Array[a] = segment.values
      ctype match {
        case CString            => new ArrayStrColumn(defined, values)
        case COffsetDateTime    => new ArrayOffsetDateTimeColumn(defined, values)
        case COffsetTime        => new ArrayOffsetTimeColumn(defined, values)
        case COffsetDate        => new ArrayOffsetDateColumn(defined, values)
        case CLocalDateTime     => new ArrayLocalDateTimeColumn(defined, values)
        case CLocalTime         => new ArrayLocalTimeColumn(defined, values)
        case CLocalDate         => new ArrayLocalDateColumn(defined, values)
        case CInterval          => new ArrayIntervalColumn(defined, values)
        case CNum               => new ArrayNumColumn(defined, values)
        case CDouble            => new ArrayDoubleColumn(defined, values)
        case CLong              => new ArrayLongColumn(defined, values)
        case cat: CArrayType[_] => new ArrayHomogeneousArrayColumn(defined, values)(cat)
        case CBoolean           => sys.error("impossible")
      }

    case BooleanSegment(_, _, defined, values, _) =>
      new ArrayBoolColumn(defined, values)

    case NullSegment(_, _, ctype, defined, _) => ctype match {
      case CNull =>
        NullColumn(defined)
      case CEmptyObject =>
        new MutableEmptyObjectColumn(defined)
      case CEmptyArray =>
        new MutableEmptyArrayColumn(defined)
      case CUndefined =>
        sys.error("also impossible")
    }
  }

  private def buildMap(segments: Seq[Segment]): Map[ColumnRef, Column] =
    segments.map(seg => (buildColumnRef(seg), buildColumn(seg))).toMap

  private val cols: Map[ColumnRef, Column] =
    buildMap(segments) + buildKeyColumn(segments.headOption map (_.length) getOrElse 0)

  val size: Int = {
    val sz = segments.foldLeft(0)(_ max _.length)
    if (logger.isTraceEnabled) {
      logger.trace("Computed size %d from:\n  %s".format(sz, segments.mkString("\n  ")))
    }
    sz
  }

  def columns: Map[ColumnRef, Column] = Slice.replaceColumnImpl(size, cols)
}

object SegmentsWrapper extends ((Seq[Segment], Int, Long) => SegmentsWrapper) {

  def sliceToSegments(id: Long, slice0: Slice): List[Segment] = {
    val slice = slice0.materialized
    val length = slice.size

    slice.columns.toList map {
      case (ColumnRef(path, CBoolean), col: ArrayBoolColumn) =>
        BooleanSegment(id, path, col.definedAt, col.values, length)

      case (ColumnRef(path, tpe: CNullType), col: BitsetColumn) =>
        NullSegment(id, path, tpe, col.definedAt, length)

      // a =:= e
      case (ColumnRef(path, tpe: CValueType[e]), col: AccessibleArrayColumn[a]) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[a]], col.definedAt, col.values)

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonBoolColumn) =>
        BooleanSegment(id, path, SingletonColumn.Defined, SingletonBoolColumn.bitset(col.value), 1)

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonLongColumn) =>
        ArraySegment[Long](id, path, tpe.asInstanceOf[CValueType[Long]], SingletonColumn.Defined, Array[Long](col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonDoubleColumn) =>
        ArraySegment[Double](id, path, tpe.asInstanceOf[CValueType[Double]], SingletonColumn.Defined, Array[Double](col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonNumColumn) =>
        ArraySegment[BigDecimal](id, path, tpe.asInstanceOf[CValueType[BigDecimal]], SingletonColumn.Defined, Array[BigDecimal](col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonStrColumn) =>
        ArraySegment[String](id, path, tpe.asInstanceOf[CValueType[String]], SingletonColumn.Defined, Array[String](col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonOffsetDateTimeColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[OffsetDateTime]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonOffsetDateColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[OffsetDate]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonOffsetTimeColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[OffsetTime]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonLocalDateTimeColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[LocalDateTime]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonLocalDateColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[LocalDate]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonLocalTimeColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[LocalTime]], SingletonColumn.Defined, Array(col.value))

      case (ColumnRef(path, tpe: CValueType[e]), col: SingletonIntervalColumn) =>
        ArraySegment(id, path, tpe.asInstanceOf[CValueType[DateTimeInterval]], SingletonColumn.Defined, Array(col.value))

      case (_, col) =>
        sys.error(s"Slice#materialized produced an unexpected column: $col")
    }
  }
}
