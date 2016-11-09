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

import scala.Predef.{ $conforms, assert }
import ygg._, common._, table._
import scala.math.Ordering
import org.specs2.matcher.MatchersImplicits._

class RowFormatSpec extends quasar.Qspec with JdbmCValueGenerators {
  import Arbitrary._

  // This should generate some jpath ids, then generate CTypes for these.
  def genJpathIds: Gen[Vec[String]] = (Gen.alphaStr filter (_.length > 0)).list map (_.toVector)
  def genColumnRefs: Gen[Vec[ColumnRef]] = genJpathIds >> { ids =>
    val generators = ids.distinct map (id => vectorOf(genCType) ^^ (_.distinct map (tp => ColumnRef(CPath(id), tp))))
    Gen.sequence(generators) ^^ (_.flatten.toVector)
  }

  def groupConsecutive[A, B](as: Vec[A])(f: A => B) = {
    @tailrec
    def build(as: Vec[A], prev: Vec[Vec[A]]): Vec[Vec[A]] = as match {
      case Seq()  => prev.reverse
      case a +: _ =>
        val bs = as takeWhile (b => f(a) == f(b))
        build(as drop bs.size, bs :: prev)
    }

    build(as, Vec())
  }

  def genCValuesForColumnRefs(refs: Vec[ColumnRef]): Gen[Vec[CValue]] = {
    val generators = groupConsecutive(refs)(_.selector) map (refs =>
                                                               genIndex(refs.size) >> (i =>
                                                                                         Gen.sequence(refs.zipWithIndex map {
                                                                                         case (ColumnRef(_, cType), `i`) =>
                                                                                           Gen.frequency(5 -> genCValue(cType), 1 -> Gen.const(CUndefined))
                                                                                         case _ => Gen.const(CUndefined)
                                                                                       })))
    (Gen sequence generators) ^^ (_.flatten.toVector)
  }

  def arrayColumnsFor(size: Int, refs: Vec[ColumnRef]): Vec[ArrayColumn[_]] =
    refs map JDBM.columnFor(CPath.Identity, size) map (_._2)

  def verify(rows: Vec[Vec[CValue]], cols: Vec[Column]) = {
    rows.zipWithIndex foreach {
      case (values, row) =>
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

  implicit val shrinkCValues: Shrink[Vec[CValue]]   = Shrink.shrinkAny[Vec[CValue]]
  implicit val shrinkRows: Shrink[Vec[Vec[CValue]]] = Shrink.shrinkAny[Vec[Vec[CValue]]]

  "ValueRowFormat" should {
    checkRoundTrips(RowFormat.forValues(_))
  }

  private def order[A](f: (A, A) => Int): Ordering[A] = new Ordering[A] {
    def compare(a: A, b: A): Int = f(a, b)
  }
  private def specFromColumnRefs[A](refs: Vec[ColumnRef])(mkArb: Vec[ColumnRef] => Gen[A])(f: A => org.specs2.execute.Result): Prop = {
    implicit val arbThing = Arbitrary(mkArb(refs))
    prop(f)
  }

  "SortingKeyRowFormat" should {
    checkRoundTrips(RowFormat.forSortingKey(_))

    "sort encoded as ValueFormat does" in prop { (refs: Vec[ColumnRef]) =>
      specFromColumnRefs(refs)(xs => vectorOfN(10, genCValuesForColumnRefs(xs))) { vals =>
        val valueRowFormat      = RowFormat.forValues(refs)
        val sortingKeyRowFormat = RowFormat.forSortingKey(refs)
        val valueEncoded        = vals map (valueRowFormat.encode(_))
        val sortEncoded         = vals map (sortingKeyRowFormat.encode(_))

        val sortedA = valueEncoded sorted order(valueRowFormat.compare) map (valueRowFormat decode _)
        val sortedB = sortEncoded sorted order(sortingKeyRowFormat.compare) map (sortingKeyRowFormat decode _)

        sortedA must_== sortedB
      }
    }.pendingUntilFixed
  }

  def checkRoundTrips(toRowFormat: Vec[ColumnRef] => RowFormat) = {
    "survive round-trip from CValue -> Array[Byte] -> CValue" in {
      prop { (refs: Vec[ColumnRef]) =>
        val rowFormat                                        = toRowFormat(refs)
        implicit val arbColumnValues: Arbitrary[Vec[CValue]] = Arbitrary(genCValuesForColumnRefs(refs))

        prop { (vals: Vec[CValue]) =>
          assert(refs.size == vals.size)
          rowFormat.decode(rowFormat.encode(vals)) must_== vals
        }
      }.flakyTest
    }
    "survive round-trip from CValue -> Array[Byte] -> Column -> Array[Byte] -> CValue" in {
      val size = 10

      prop { (refs: Vec[ColumnRef]) =>
        val rowFormat = toRowFormat(refs)
        implicit val arbRows: Arbitrary[Vec[Vec[CValue]]] =
          Arbitrary(vectorOfN(size, genCValuesForColumnRefs(refs)))

        prop { (rows: Vec[Vec[CValue]]) =>
          val columns       = arrayColumnsFor(size, refs)
          val columnDecoder = rowFormat.ColumnDecoder(columns)
          val columnEncoder = rowFormat.ColumnEncoder(columns)

          // Fill up the columns with the values from the rows.
          rows.zipWithIndex foreach {
            case (vals, row) =>
              columnDecoder.decodeToRow(row, rowFormat.encode(vals), offset = 0)
          }

          verify(rows, columns)

          rows.zipWithIndex forall {
            case (vals, row) =>
              rowFormat.decode(columnEncoder.encodeFromRow(row)) must_=== vals
          }
        }
      }.flakyTest
    }
  }
}
