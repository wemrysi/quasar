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

import ygg.common._

class DerefSliceOps(private val source: Slice) extends AnyVal {
  def dereferencedColumns(derefBy: Int =?> CPathNode): Map[ColumnRef, Column] = {
    val forwardIndex: Map[CPathNode, Map[ColumnRef, Column]] = source.columns.foldLeft(Map.empty[CPathNode, Map[ColumnRef, Column]]) {
      case (acc, (ColumnRef(CPath(root, xs @ _ *), ctype), col)) =>
        val resultRef = ColumnRef(CPath(xs: _*), ctype)
        // we know the combination of xs and ctype to be unique within root
        acc + (root -> (acc.getOrElse(root, Map()) + (resultRef -> col)))
    }

    val indexableArrays: Map[ColumnRef, HomogeneousArrayColumn[_]] = source.columns.collect {
      case (ColumnRef(CPath(CPathArray, xs @ _ *), ctype), col: HomogeneousArrayColumn[_]) =>
        (ColumnRef(CPath(xs: _*), ctype), col)
    }.toMap

    def derefColumns(node: CPathNode): Option[Map[ColumnRef, Column]] = node match {
      case CPathIndex(i) if !indexableArrays.isEmpty =>
        Some((indexableArrays mapValues (_.select(i))) ++ forwardIndex.getOrElse(node, Map.empty))
      case _ => forwardIndex get node
    }

    source.columns.keySet.foldLeft(Map.empty[ColumnRef, Column]) {
      case (acc, ColumnRef(CPath(_, xs @ _ *), ctype)) =>
        val resultRef = ColumnRef(CPath(xs: _*), ctype)

        lazy val resultCol = ctype match {
          case CBoolean =>
            new BoolColumn {
              private var row0: Int           = -1
              private var refCol0: BoolColumn = _
              @inline private def refCol(row: Int): BoolColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[BoolColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CLong =>
            new LongColumn {
              private var row0: Int           = -1
              private var refCol0: LongColumn = _
              @inline private def refCol(row: Int): LongColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[LongColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CDouble =>
            new DoubleColumn {
              private var row0: Int             = -1
              private var refCol0: DoubleColumn = _
              @inline private def refCol(row: Int): DoubleColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DoubleColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CNum =>
            new NumColumn {
              private var row0: Int          = -1
              private var refCol0: NumColumn = _
              @inline private def refCol(row: Int): NumColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[NumColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CString =>
            new StrColumn {
              private var row0: Int          = -1
              private var refCol0: StrColumn = _
              @inline private def refCol(row: Int): StrColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[StrColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CDate =>
            new DateColumn {
              private var row0: Int           = -1
              private var refCol0: DateColumn = _
              @inline private def refCol(row: Int): DateColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[DateColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CPeriod =>
            new PeriodColumn {
              private var row0: Int             = -1
              private var refCol0: PeriodColumn = _
              @inline private def refCol(row: Int): PeriodColumn =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[PeriodColumn]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case cArrayType: CArrayType[a] =>
            new HomogeneousArrayColumn[a] {
              val tpe                                        = cArrayType
              private var row0: Int                          = -1
              private var refCol0: HomogeneousArrayColumn[a] = _
              @inline private def refCol(row: Int): HomogeneousArrayColumn[a] =
                derefColumns(derefBy(row)).flatMap(_.get(resultRef)).orNull.asInstanceOf[HomogeneousArrayColumn[a]]

              def apply(row: Int) = refCol0(row)

              def isDefinedAt(row: Int) = {
                derefBy.isDefinedAt(row) && {
                  if (row0 != row) { row0 = row; refCol0 = refCol(row) }
                  refCol0 != null && refCol0.isDefinedAt(row)
                }
              }
            }

          case CEmptyObject =>
            new EmptyObjectColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CEmptyArray =>
            new EmptyArrayColumn {
              def isDefinedAt(row: Int) =
                derefBy.isDefinedAt(row) &&
                  derefColumns(derefBy(row)).exists(_.get(resultRef).exists(_.isDefinedAt(row)))
            }

          case CNull =>
            new NullColumn {
              def isDefinedAt(row: Int) = derefBy.isDefinedAt(row) && derefColumns(derefBy(row)).exists(cols => cols(resultRef).isDefinedAt(row))
            }

          case CUndefined => UndefinedColumn.raw
        }

        acc + (resultRef -> acc.getOrElse(resultRef, resultCol))
    }
  }
}
