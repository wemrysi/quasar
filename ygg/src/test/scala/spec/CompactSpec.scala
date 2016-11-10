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

import scala.Predef.$conforms
import ygg.cf
import ygg.common._
import scala.util.Random
import ygg.table._
import ygg.data._

final case class TableStats[T: TableRep](val table: T) {
  val slices    = table.slicesStream.toVector

  def stats: Vector[Int -> Int] = slices map { slice =>
    val size  = slice.size
    val undef = 0 until size count (i => !slice.columns.values.exists(_ isDefinedAt i))

    size -> undef
  }
  def sliceSizes  = stats map (_._1)
  def undefCounts = stats map (_._2)
  def summary     = undefCounts.sum -> sliceSizes.count(_ == 0)
}

class CompactSpec extends TableQspec {
  import SampleData._
  import trans._

  implicit val tableGen: Arbitrary[Table] = sample(schema) ^^ fromSample

  "in compact" >> {
    "be the identity on fully defined tables"    in prop(testIdentity _)
    "preserve all defined rows"                  in prop(testCompactIdentity _)
    "preserve all defined key rows"              in prop(testCompactPreserveKey _)
    "have no undefined rows or empty slices"     in prop(testCompactRows _)
    "have no undefined key rows or empty slices" in prop(testCompactKeyRows _)
  }

  private def chooseColumn(cTable: Table): TransSpec1 =
    cTable.slicesStream.headOption.fold(ID: TransSpec1)(slice =>
      ID \ Random.shuffle(slice.columns.keys.map(_.selector)).head
    )

  // fuzzing must be done strictly otherwise sadness will ensue (???)
  private def fuzzSlices(table: Table)(f: Slice => Slice): Table =
    lazyTable[Table](table.slicesStream map f, UnknownSize)

  private def testTableInvariant[A](table: Table)(f1: Table => A, f2: Table => A) =
    f1(table) must_=== f2(table)

  private def testSliceInvariant(table: Table)(fuzz: Slice => Slice) =
    table.toVector must_=== fuzzSlices(table)(fuzz).toVector

  private def undefineTable(cTable: Table): Table = fuzzSlices(cTable) { slice =>
    val sz = slice.size
    val bs =
      (
        if (cTable.slicesCount > 1 && randomDouble < 0.25)
          Bits()
        else
          Bits(0 until sz filter (_ => randomDouble < 0.75))
      )
    Slice(sz, slice.columns mapValues (_ |> cf.filter(0, sz, bs) get))
  }

  private def undefineColumn(cTable: Table, path: CPath): Table = fuzzSlices(cTable) { slice =>
    val colRef = slice.columns.keys.find(_.selector == path)
    val maskedSlice = colRef.map { colRef =>
      val col = slice.columns(colRef)
      val maskedCol =
        if (cTable.slicesCount > 1 && randomDouble < 0.25)
          (col |> cf.filter(0, slice.size, new BitSet)).get
        else {
          val retained = (0 until slice.size).map { (x: Int) =>
            if (randomDouble < 0.75) Some(x) else None
          }.flatten
          (col |> cf.filter(0, slice.size, Bits(retained))).get
        }

      Slice(slice.size, slice.columns.updated(colRef, maskedCol))
    }
    maskedSlice.getOrElse(slice)
  }

  private def testIdentity(table: Table)           = testTableInvariant(table)(x => x, _ compact ID)
  private def testCompactIdentity(table: Table)    = testTableInvariant(undefineTable(table))(x => x, _ compact ID)
  private def testCompactPreserveKey(table: Table) = chooseColumn(table) |> (key =>
    testTableInvariant(undefineColumn(table, key.extractPath))(
      _ transform key,
      _ compact key transform key
    )
  )
  private def testCompactRows(table: Table) = testTableInvariant(undefineTable(table))(
    t => TableStats(t compact ID).summary,
    _ => ((0, 0))
  )
  private def testCompactKeyRows(table: Table) = chooseColumn(table) |> (key =>
    testTableInvariant(undefineColumn(table, key.extractPath))(
      t => TableStats(t compact key transform key).summary,
      _ => ((0, 0))
    )
  )
}
