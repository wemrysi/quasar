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
import scalaz.{ =?> => _, _ }

sealed trait Slice {
  def size: Int
  def columns: ColumnMap
}
final case class DirectSlice(size: Int, columns: ColumnMap) extends Slice
final class DerefSlice(source: Slice, derefBy: Int =?> CPathNode) extends Slice {
  val size    = source.size
  val columns = source dereferencedColumns derefBy
}

object EmptySlice {
  def unapply(x: Slice): Boolean = x.size == 0
}

object Slice {
  implicit def sliceOps(s: Slice): SliceOps = new SliceOps(s)

  def empty: Slice                                = apply(0, columnMap())
  def apply(size: Int, columns: ColumnMap): Slice = new DirectSlice(size, columns)
  def apply(pair: ColumnMap -> Int): Slice        = apply(pair._2, pair._1)
  def unapply(x: Slice)                           = Some(x.size -> x.columns)

  def updateRefs(rv: RValue, into: ArrayColumnMap, sliceIndex: Int, sliceSize: Int): ArrayColumnMap = {
    rv.flattenWithPath.foldLeft(into) {
      case (acc, (cpath, CUndefined)) => acc
      case (acc, (cpath, cvalue)) =>
        val ref = ColumnRef(cpath, (cvalue.cType))

        val updatedColumn: ArrayColumn[_] = cvalue match {
          case CBoolean(b) =>
            acc.getOrElse(ref, ArrayBoolColumn.empty()).asInstanceOf[ArrayBoolColumn].unsafeTap { c =>
              c.update(sliceIndex, b)
            }

          case CLong(d) =>
            acc.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn].unsafeTap { c =>
              c.update(sliceIndex, d.toLong)
            }

          case CDouble(d) =>
            acc.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn].unsafeTap { c =>
              c.update(sliceIndex, d.toDouble)
            }

          case CNum(d) =>
            acc.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn].unsafeTap { c =>
              c.update(sliceIndex, d)
            }

          case CString(s) =>
            acc.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn].unsafeTap { c =>
              c.update(sliceIndex, s)
            }

          case CDate(d) =>
            acc.getOrElse(ref, ArrayDateColumn.empty(sliceSize)).asInstanceOf[ArrayDateColumn].unsafeTap { c =>
              c.update(sliceIndex, d)
            }

          case CPeriod(p) =>
            acc.getOrElse(ref, ArrayPeriodColumn.empty(sliceSize)).asInstanceOf[ArrayPeriodColumn].unsafeTap { c =>
              c.update(sliceIndex, p)
            }

          case CArray(arr, cType) =>
            acc.getOrElse(ref, ArrayHomogeneousArrayColumn.empty(sliceSize)(cType)).asInstanceOf[ArrayHomogeneousArrayColumn[cType.tpe]].unsafeTap { c =>
              c.update(sliceIndex, arr)
            }

          case CEmptyArray =>
            acc.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }

          case CEmptyObject =>
            acc.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }

          case CNull =>
            acc.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }
          case x =>
            abort(s"Unexpected arg $x")
        }
        acc.updated(ref, updatedColumn)
    }
  }

  def fromJValues(values: Stream[JValue]): Slice = fromRValues(values.map(RValue.fromJValue))

  def fromRValues(values: Stream[RValue]): Slice = {
    val sliceSize = values.size

    @tailrec def buildColArrays(from: Stream[RValue], into: ArrayColumnMap, sliceIndex: Int): (ArrayColumnMap, Int) = {
      from match {
        case jv #:: xs =>
          val refs = updateRefs(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (refs, size) = buildColArrays(values, Map(), 0)
    Slice(size, EagerColumnMap(refs.toVector))
  }

  /**
    * Concatenate multiple slices into 1 big slice. The slices will be
    * concatenated in the order they appear in `slices`.
    */
  def concat(slices: Seq[Slice]): Slice = {
    val (_columns, _size) = slices.foldLeft((Map.empty[ColumnRef, List[Int -> Column]], 0)) {
      case ((cols, offset), slice) if slice.size > 0 =>
        (slice.columns.foldLeft(cols) {
          case (acc, (ref, col)) =>
            acc + (ref -> ((offset, col) :: acc.getOrElse(ref, Nil)))
        }, offset + slice.size)

      case ((cols, offset), _) => (cols, offset)
    }

    Slice(_size, _columns flatMap { case (ref, parts) => NConcat(parts) map (ref -> _) })
  }

  def rowComparatorFor(s1: Slice, s2: Slice)(keyf: Slice => Iterable[CPath]): RowComparator = {
    val paths     = (keyf(s1) ++ keyf(s2)).toVector
    val traversal = CPathTraversal(paths.toList)
    val lCols     = s1.columns groupBy (_._1.selector) map { case (path, m) => path -> m.values.toSet }
    val rCols     = s2.columns groupBy (_._1.selector) map { case (path, m) => path -> m.values.toSet }
    val allPaths  = lCols.keys ++ rCols.keys toList
    val order     = traversal.rowOrder(allPaths, lCols, Some(rCols))

    new RowComparator {
      def compare(r1: Int, r2: Int): Ordering = order.order(r1, r2)
    }
  }

  /**
    * Given a JValue, an existing map of columnrefs to column data,
    * a sliceIndex, and a sliceSize, return an updated map.
    */
  def withIdsAndValues(jv: JValue, into: ArrayColumnMap, sliceIndex: Int, sliceSize: Int): ArrayColumnMap = {
    jv.flattenWithPath.foldLeft(into) {
      case (acc, (jpath, JUndefined)) => acc
      case (acc, (jpath, v)) =>
        val ctype = CType.forJValue(v) getOrElse { abort("Cannot determine ctype for " + v + " at " + jpath + " in " + jv) }
        val ref   = ColumnRef(CPath(jpath), ctype)

        val updatedColumn: ArrayColumn[_] = v match {
          case JBool(b) =>
            acc.getOrElse(ref, ArrayBoolColumn.empty()).asInstanceOf[ArrayBoolColumn].unsafeTap { c =>
              c.update(sliceIndex, b)
            }

          case JNum(d) =>
            ctype match {
              case CLong =>
                acc.getOrElse(ref, ArrayLongColumn.empty(sliceSize)).asInstanceOf[ArrayLongColumn].unsafeTap { c =>
                  c.update(sliceIndex, d.toLong)
                }

              case CDouble =>
                acc.getOrElse(ref, ArrayDoubleColumn.empty(sliceSize)).asInstanceOf[ArrayDoubleColumn].unsafeTap { c =>
                  c.update(sliceIndex, d.toDouble)
                }

              case CNum =>
                acc.getOrElse(ref, ArrayNumColumn.empty(sliceSize)).asInstanceOf[ArrayNumColumn].unsafeTap { c =>
                  c.update(sliceIndex, d)
                }

              case _ => abort("non-numeric type reached")
            }

          case JString(s) =>
            acc.getOrElse(ref, ArrayStrColumn.empty(sliceSize)).asInstanceOf[ArrayStrColumn].unsafeTap { c =>
              c.update(sliceIndex, s)
            }

          case JArray(Seq()) =>
            acc.getOrElse(ref, MutableEmptyArrayColumn.empty()).asInstanceOf[MutableEmptyArrayColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }

          case JObject.empty =>
            acc.getOrElse(ref, MutableEmptyObjectColumn.empty()).asInstanceOf[MutableEmptyObjectColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }

          case JNull =>
            acc.getOrElse(ref, MutableNullColumn.empty()).asInstanceOf[MutableNullColumn].unsafeTap { c =>
              c.update(sliceIndex, true)
            }

          case _ => abort("non-flattened value reached")
        }

        acc + (ref -> updatedColumn)
    }
  }
}
