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
package quasar.ygg

import blueeyes._
import com.precog.common._
import quasar.ygg.table._
import org.scalacheck.Shrink
import quasar.precog.TestSupport._

class RowFormatSpec extends quasar.Qspec with JdbmCValueGenerators {
  import Arbitrary._

  // This should generate some jpath ids, then generate CTypes for these.
  def genJpathIds: Gen[List[String]] = Gen.alphaStr filter (_.length > 0) list
  def genColumnRefs: Gen[List[ColumnRef]] = genJpathIds >> { ids =>
    val generators = ids.distinct map (id => listOf(genCType) ^^ (_.distinct map (tp => ColumnRef(CPath(id), tp))))
    Gen.sequence(generators) ^^ (_.flatten.toList)
  }

  def groupConsecutive[A, B](as: List[A])(f: A => B) = {
    @tailrec
    def build(as: List[A], prev: List[List[A]]): List[List[A]] = as match {
      case Nil =>
        prev.reverse
      case a :: _ =>
        val bs = as takeWhile { b => f(a) == f(b) }
        build(as drop bs.size, bs :: prev)
    }

    build(as, Nil)
  }

  def genCValuesForColumnRefs(refs: List[ColumnRef]): Gen[List[CValue]] = {
    val generators = groupConsecutive(refs)(_.selector) map (refs =>
      genIndex(refs.size) >> (i =>
        Gen.sequence(refs.zipWithIndex map {
          case (ColumnRef(_, cType), `i`) => Gen.frequency(5 -> genCValue(cType), 1 -> Gen.const(CUndefined))
          case _                          => Gen.const(CUndefined)
        })
      )
    )
    (Gen sequence generators) ^^ (_.flatten.toList)
  }

  def arrayColumnsFor(size: Int, refs: List[ColumnRef]): List[ArrayColumn[_]] =
    refs map JDBMSlice.columnFor(CPath.Identity, size) map (_._2)

  def verify(rows: List[List[CValue]], cols: List[Column]) = {
    rows.zipWithIndex foreach { case (values, row) =>
      (values zip cols) foreach (_ must beLike {
        case (CUndefined, col) if !col.isDefinedAt(row)                              => ok
        case (_, col) if !col.isDefinedAt(row)                                       => ko
        case (CString(s), col: StrColumn)                                            => col(row) must_== s
        case (CBoolean(x), col: BoolColumn)                                          => col(row) must_== x
        case (CLong(x), col: LongColumn)                                             => col(row) must_== x
        case (CDouble(x), col: DoubleColumn)                                         => col(row) must_== x
        case (CNum(x), col: NumColumn)                                               => col(row) must_== x
        case (CDate(x), col: DateColumn)                                             => col(row) must_== x
        case (CNull, col: NullColumn)                                                => ok
        case (CEmptyObject, col: EmptyObjectColumn)                                  => ok
        case (CEmptyArray, col: EmptyArrayColumn)                                    => ok
        case (CArray(xs, cType), col: HomogeneousArrayColumn[_]) if cType == col.tpe => col(row) must_== xs
      })
    }
  }

  implicit lazy val arbColumnRefs = Arbitrary(genColumnRefs)

  implicit val shrinkCValues: Shrink[List[CValue]] = Shrink.shrinkAny[List[CValue]]
  implicit val shrinkRows: Shrink[List[List[CValue]]] = Shrink.shrinkAny[List[List[CValue]]]

  "ValueRowFormat" should {
    checkRoundTrips(RowFormat.forValues(_))
  }

  private def identityCols(len: Int): List[ColumnRef] = (0 until len).map({ i =>
    ColumnRef(CPath(CPathIndex(i)), CLong)
  })(scala.collection.breakOut)

  "IdentitiesRowFormat" should {
    "round-trip CLongs" in {
      prop { id: List[Long] =>
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val cId: List[CValue] = id map (CLong(_))
        rowFormat.decode(rowFormat.encode(cId)) must_== cId
      }
    }

    "encodeIdentities matches encode format" in {
      prop { id: List[Long] =>
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val cId: List[CValue] = id map (CLong(_))
        rowFormat.decode(rowFormat.encodeIdentities(id.toArray)) must_== cId
      }
    }

    "round-trip CLongs -> Column -> CLongs" in {
      prop { id: List[Long] =>
        val columns = arrayColumnsFor(1, identityCols(id.size))
        val rowFormat = RowFormat.IdentitiesRowFormatV1(identityCols(id.size))
        val columnDecoder = rowFormat.ColumnDecoder(columns)
        val columnEncoder = rowFormat.ColumnEncoder(columns)

        val cId: List[CValue] = id map (CLong(_))
        columnDecoder.decodeToRow(0, rowFormat.encode(cId))

        verify(cId :: Nil, columns)

        rowFormat.decode(columnEncoder.encodeFromRow(0)) must_== cId
      }
    }
  }

  private def order[A](f: (A, A) => Int): Ordering[A] = new Ordering[A] {
    def compare(a: A, b: A): Int = f(a, b)
  }
  private def specFromColumnRefs[A](refs: List[ColumnRef])(mkArb: List[ColumnRef] => Gen[A])(f: A => org.specs2.execute.Result): Prop = {
    implicit val arbThing = Arbitrary(mkArb(refs))
    prop(f)
  }

  "SortingKeyRowFormat" should {
    checkRoundTrips(RowFormat.forSortingKey(_))

    "sort encoded as ValueFormat does" in prop { (refs: List[ColumnRef]) =>
      specFromColumnRefs(refs)(xs => Gen.listOfN(10, genCValuesForColumnRefs(xs))) { vals =>
        val valueRowFormat      = RowFormat.forValues(refs)
        val sortingKeyRowFormat = RowFormat.forSortingKey(refs)
        val valueEncoded        = vals map (valueRowFormat.encode(_))
        val sortEncoded         = vals map (sortingKeyRowFormat.encode(_))

        val sortedA = valueEncoded sorted order(valueRowFormat.compare) map (valueRowFormat decode _)
        val sortedB = sortEncoded sorted order(sortingKeyRowFormat.compare) map (sortingKeyRowFormat decode _)

        sortedA must_== sortedB
      }
    }
  }

  def checkRoundTrips(toRowFormat: List[ColumnRef] => RowFormat) = {
    "survive round-trip from CValue -> Array[Byte] -> CValue" in {
      prop { (refs: List[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
        implicit val arbColumnValues: Arbitrary[List[CValue]] = Arbitrary(genCValuesForColumnRefs(refs))

        prop { (vals: List[CValue]) =>
          assert(refs.size == vals.size)
          rowFormat.decode(rowFormat.encode(vals)) must_== vals
        }
      }
    }
    "survive round-trip from CValue -> Array[Byte] -> Column -> Array[Byte] -> CValue" in {
      val size = 10

      prop { (refs: List[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
        implicit val arbRows: Arbitrary[List[List[CValue]]] =
          Arbitrary(Gen.listOfN(size, genCValuesForColumnRefs(refs)))

        prop { (rows: List[List[CValue]]) =>
          val columns = arrayColumnsFor(size, refs)
          val columnDecoder = rowFormat.ColumnDecoder(columns)
          val columnEncoder = rowFormat.ColumnEncoder(columns)

          // Fill up the columns with the values from the rows.
          rows.zipWithIndex foreach { case (vals, row) =>
            columnDecoder.decodeToRow(row, rowFormat.encode(vals))
          }

          verify(rows, columns)

          rows.zipWithIndex forall { case (vals, row) =>
            rowFormat.decode(columnEncoder.encodeFromRow(row)) must_== vals
          }
        }
      }
    }
  }
}
