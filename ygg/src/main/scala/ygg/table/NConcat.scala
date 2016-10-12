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

import ygg._, common._, data._

object ConcatHelpers {
  def buildFilters(columns: ColumnMap, size: Int, filter: ColumnMap => ColumnMap, filterEmpty: ColumnMap => ColumnMap) = {
    val definedBits = filter(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    val emptyBits   = filterEmpty(columns).values.map(_.definedAt(0, size)).reduceOption(_ | _) getOrElse new BitSet
    (definedBits, emptyBits)
  }

  def buildOuterBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet): BitSet = {
    (rightEmptyBits & leftEmptyBits) |
      (rightEmptyBits &~ leftDefinedBits) |
      (leftEmptyBits &~ rightDefinedBits)
  }

  def buildInnerBits(leftEmptyBits: BitSet, rightEmptyBits: BitSet, leftDefinedBits: BitSet, rightDefinedBits: BitSet) = {
    val emptyBits    = rightEmptyBits & leftEmptyBits
    val nonemptyBits = leftDefinedBits & rightDefinedBits
    (emptyBits, nonemptyBits)
  }

  def filterArrays(columns: ColumnMap) = columns filter {
    case (ColumnRef(CPath(CPathIndex(_), _ @_ *), _), _) => true
    case (ColumnRef.id(CEmptyArray), _)                  => true
    case _                                               => false
  }

  def filterEmptyArrays(columns: ColumnMap) = columns filter {
    case (ColumnRef.id(CEmptyArray), _) => true
    case _                              => false
  }

  def collectIndices(columns: ColumnMap) = columns.fields.collect {
    case (ref @ ColumnRef(CPath(CPathIndex(i), xs @ _ *), ctype), col) => (i, xs, ref, col)
  }

  def buildEmptyArrays(emptyBits: BitSet) = Map(ColumnRef.id(CEmptyArray) -> EmptyArrayColumn(emptyBits))

  def buildNonemptyArrays(left: ColumnMap, right: ColumnMap) = {
    val leftIndices  = collectIndices(left)
    val rightIndices = collectIndices(right)

    val maxId = if (leftIndices.isEmpty) -1 else leftIndices.map(_._1).max
    val newCols = (
         (leftIndices map { case (_, _, ref, col) => ref -> col })
      ++ (rightIndices map { case (i, xs, ref, col) => ColumnRef(CPath(CPathIndex(i + maxId + 1) +: xs.toVector), ref.ctype) -> col })
    )

    newCols.toMap
  }

  def filterObjects(columns: ColumnMap) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @_ *), _), _) => true
    case (ColumnRef.id(CEmptyObject), _)                 => true
    case _                                               => false
  }

  def filterEmptyObjects(columns: ColumnMap) = columns.filter {
    case (ColumnRef.id(CEmptyObject), _) => true
    case _                               => false
  }

  def filterFields(columns: ColumnMap) = columns.filter {
    case (ColumnRef(CPath(CPathField(_), _ @_ *), _), _) => true
    case _                                               => false
  }

  def buildFields(leftColumns: ColumnMap, rightColumns: ColumnMap) =
    (filterFields(leftColumns), filterFields(rightColumns))

  def buildEmptyObjects(emptyBits: BitSet) = (
    if (emptyBits.isEmpty) Map()
    else Map(ColumnRef.id(CEmptyObject) -> EmptyObjectColumn(emptyBits))
  )

  def buildNonemptyObjects(leftFields: ColumnMap, rightFields: ColumnMap) = {
    val (leftInner, leftOuter) = leftFields partition {
      case (ColumnRef(path, _), _)                         =>
        rightFields exists { case (ColumnRef(path2, _), _) => path == path2 }
    }

    val (rightInner, rightOuter) = rightFields partition {
      case (ColumnRef(path, _), _)                        =>
        leftFields exists { case (ColumnRef(path2, _), _) => path == path2 }
    }

    val innerPaths = Set(leftInner.keys map { _.selector } toSeq: _*)

    val mergedPairs: Set[ColumnRef -> Column] = innerPaths flatMap { path =>
      val rightSelection = rightInner filter {
        case (ColumnRef(path2, _), _) => path == path2
      }

      val leftSelection = leftInner filter {
        case (ref @ ColumnRef(path2, _), _) =>
          path == path2 && !rightSelection.contains(ref)
      }

      val rightMerged = rightSelection map {
        case (ref, col) => {
          if (leftInner contains ref)
            ref -> cf.UnionRight(leftInner(ref), col).get
          else
            ref -> col
        }
      }

      rightMerged ++ leftSelection
    }

    leftOuter ++ rightOuter ++ mergedPairs
  }
}

final object NConcat {
  // Closest thing we can get to casting an array. This is completely unsafe.
  private def copyCastArray[A: CTag](as: Array[_]): Array[A] = {
    val bs = new Array[A](as.length)
    systemArraycopy(as, 0, bs, 0, as.length)
    bs
  }

  def apply(cols: List[Int -> Column]) = {
    val sortedCols             = cols.sortBy(_._1)
    val offsets: Array[Int]    = sortedCols.map(_._1)(breakOut)
    val columns: Array[Column] = sortedCols.map(_._2)(breakOut)

    cols match {
      case (_, _: BoolColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[BoolColumn]) =>
        val boolColumns = copyCastArray[BoolColumn](columns)
        Some(new NConcatColumn(offsets, boolColumns) with BoolColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            boolColumns(i)(row - offsets(i))
          }
        })

      case (_, _: LongColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[LongColumn]) =>
        val longColumns = copyCastArray[LongColumn](columns)
        Some(new NConcatColumn(offsets, longColumns) with LongColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            longColumns(i)(row - offsets(i))
          }
        })

      case (_, _: DoubleColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DoubleColumn]) =>
        val doubleColumns = copyCastArray[DoubleColumn](columns)
        Some(new NConcatColumn(offsets, doubleColumns) with DoubleColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            doubleColumns(i)(row - offsets(i))
          }
        })

      case (_, _: NumColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NumColumn]) =>
        val numColumns = copyCastArray[NumColumn](columns)
        Some(new NConcatColumn(offsets, numColumns) with NumColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            numColumns(i)(row - offsets(i))
          }
        })

      case (_, _: StrColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[StrColumn]) =>
        val strColumns = copyCastArray[StrColumn](columns)
        Some(new NConcatColumn(offsets, strColumns) with StrColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            strColumns(i)(row - offsets(i))
          }
        })

      case (_, _: DateColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[DateColumn]) =>
        val dateColumns = copyCastArray[DateColumn](columns)
        Some(new NConcatColumn(offsets, dateColumns) with DateColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            dateColumns(i)(row - offsets(i))
          }
        })

      case (_, _: PeriodColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[PeriodColumn]) =>
        val periodColumns = copyCastArray[PeriodColumn](columns)
        Some(new NConcatColumn(offsets, periodColumns) with PeriodColumn {
          def apply(row: Int) = {
            val i = indexOf(row)
            periodColumns(i)(row - offsets(i))
          }
        })

      case (_, _: EmptyArrayColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyArrayColumn]) =>
        val emptyArrayColumns = copyCastArray[EmptyArrayColumn](columns)
        Some(new NConcatColumn(offsets, emptyArrayColumns) with EmptyArrayColumn)

      case (_, _: EmptyObjectColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[EmptyObjectColumn]) =>
        val emptyObjectColumns = copyCastArray[EmptyObjectColumn](columns)
        Some(new NConcatColumn(offsets, emptyObjectColumns) with EmptyObjectColumn)

      case (_, _: NullColumn) :: _ if Loop.forall(columns)(_.isInstanceOf[NullColumn]) =>
        val nullColumns = copyCastArray[NullColumn](columns)
        Some(new NConcatColumn(offsets, nullColumns) with NullColumn)

      case _ => None
    }
  }
}
