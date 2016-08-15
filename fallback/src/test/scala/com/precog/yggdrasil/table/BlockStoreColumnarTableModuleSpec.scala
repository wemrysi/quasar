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
package table

import TableModule._

class BlockStoreColumnarTableModuleSpec extends TableModuleSpec with BlockLoadSpec with BlockSortSpec with BlockAlignBaseSpec {
  type MemoId = Int

  "a block store columnar table" should {
    "load" >> {
      "a problem sample1" in testLoadSample1
      "a problem sample2" in testLoadSample2
      "a problem sample3" in testLoadSample3
      "a problem sample4" in testLoadSample4
      //"a problem sample5" in testLoadSample5 //pathological sample in the case of duplicated ids.
      //"a dense dataset" in checkLoadDense //scalacheck + numeric columns = pain
    }
    "sort" >> {
      "fully homogeneous data"        in homogeneousSortSample
      "fully homogeneous data with object" in homogeneousSortSampleWithNonexistentSortKey
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys"       in heterogeneousSortSample
      "heterogeneous sort keys case 2" in heterogeneousSortSample2
      "heterogeneous sort keys ascending" in heterogeneousSortSampleAscending
      "heterogeneous sort keys descending" in heterogeneousSortSampleDescending
      "top-level hetereogeneous values" in heterogeneousBaseValueTypeSample
      "sort with a bad schema"        in badSchemaSortSample
      "merges over three cells"       in threeCellMerge
      "empty input"                   in emptySort
      "with uniqueness for keys"      in uniqueSort

      "arbitrary datasets"            in checkSortDense(SortAscending)
      "arbitrary datasets descending" in checkSortDense(SortDescending)
    }
  }
}
