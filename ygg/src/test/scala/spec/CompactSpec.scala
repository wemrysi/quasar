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
import scalaz._, Scalaz._
import ygg.table._
import ygg.data._

class CompactSpec extends TableQspec {
  import SampleData._
  import trans._

  "in compact" >> {
    "be the identity on fully defined tables"  in testCompactIdentity
    "preserve all defined rows"                in testCompactPreserve
    "have no undefined rows"                   in testCompactRows
    "have no empty slices"                     in testCompactSlices
    "preserve all defined key rows"            in testCompactPreserveKey
    "have no undefined key rows"               in testCompactRowsKey
    "have no empty key slices"                 in testCompactSlicesKey
  }

  private def tableStats(cTable: Table): List[Int -> Int] = {
    val slices = cTable.slicesStream
    val sizes  = slices.map(_.size).toList
    val undefined = slices.map { slice =>
      (0 until slice.size).foldLeft(0) {
        case (acc, i) => if (!slice.columns.values.exists(_.isDefinedAt(i))) acc + 1 else acc
      }
    }.toList

    sizes zip undefined
  }

  private def mkDeref(path: CPath): TransSpec1 = {
    def mkDeref0(nodes: Seq[CPathNode]): TransSpec1 = nodes match {
      case (f: CPathField) +: rest => DerefObjectStatic(mkDeref0(rest), f)
      case (i: CPathIndex) +: rest => DerefArrayStatic(mkDeref0(rest), i)
      case _                       => root
    }

    mkDeref0(path.nodes)
  }

  private def extractPath(spec: TransSpec1): Option[CPath] = spec match {
    case DerefObjectStatic(TransSpec1.Id, f) => Some(f)
    case DerefObjectStatic(lhs, f)           => extractPath(lhs).map(_ \ f)
    case DerefArrayStatic(TransSpec1.Id, i)  => Some(i)
    case DerefArrayStatic(lhs, f)            => extractPath(lhs).map(_ \ f)
    case _                                   => None
  }

  private def chooseColumn(cTable: Table): TransSpec1 = {
    cTable.slicesStream.headOption.map { slice =>
      val chosenPath = Random.shuffle(slice.columns.keys.map(_.selector)).head
      mkDeref(chosenPath)
    } getOrElse (mkDeref(CPath.Identity))
  }

  private def undefineTable(cTable: Table): Table = {
    val slices    = cTable.slicesStream // fuzzing must be done strictly otherwise sadness will ensue
    val numSlices = slices.size

    val maskedSlices = slices map { slice =>
      val sz = slice.size
      val bs =
        (
          if (numSlices > 1 && randomDouble < 0.25)
            Bits()
          else
            Bits(0 until sz filter (_ => randomDouble < 0.75))
        )
      Slice(sz, slice.columns mapValues (_ |> cf.filter(0, sz, bs) get))
    }
    lazyTable[Table](maskedSlices, UnknownSize)
  }

  private def undefineColumn(cTable: Table, path: CPath): Table = {
    val slices    = cTable.slicesStream // fuzzing must be done strictly otherwise sadness will ensue
    val numSlices = slices.size

    val maskedSlices = slices.map { slice =>
      val colRef = slice.columns.keys.find(_.selector == path)
      val maskedSlice = colRef.map { colRef =>
        val col = slice.columns(colRef)
        val maskedCol =
          if (numSlices > 1 && randomDouble < 0.25)
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

    lazyTable[Table](maskedSlices, UnknownSize)
  }

  private def testCompactIdentity = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table        = fromSample(sample)
      val compactTable = table.compact(Leaf(Source))

      val results = toJson(compactTable)

      results.copoint must_== sample.data
    }
  }

  private def testCompactPreserve = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson  = toJson(sampleTable)

      val compactTable = sampleTable.compact(Leaf(Source))
      val results      = toJson(compactTable)

      results.copoint must_== sampleJson.copoint
    }
  }

  private def testCompactRows = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))

      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)

      compactStats.map(_._2).foldLeft(0)(_ + _) must_== 0
    }
  }

  private def testCompactSlices = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))

      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)

      compactStats.map(_._1).count(_ == 0) must_== 0
    }
  }

  private def testCompactPreserveKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key       = chooseColumn(baseTable)

      val sampleTable   = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))
      val sampleKey     = sampleTable.transform(key)
      val sampleKeyJson = toJson(sampleKey)

      val compactTable  = sampleTable.compact(key)
      val resultKey     = compactTable.transform(key)
      val resultKeyJson = toJson(resultKey)

      resultKeyJson.copoint must_== sampleKeyJson.copoint
    }
  }

  private def testCompactRowsKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key       = chooseColumn(baseTable)

      val sampleTable = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))

      val compactTable   = sampleTable.compact(key)
      val resultKey      = compactTable.transform(key)
      val resultKeyStats = tableStats(resultKey)

      resultKeyStats.map(_._2).foldLeft(0)(_ + _) must_== 0
    }
  }

  private def testCompactSlicesKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key       = chooseColumn(baseTable)

      val sampleTable = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))

      val compactTable   = sampleTable.compact(key)
      val resultKey      = compactTable.transform(key)
      val resultKeyStats = tableStats(resultKey)

      resultKeyStats.map(_._1).count(_ == 0) must_== 0
    }
  }
}
