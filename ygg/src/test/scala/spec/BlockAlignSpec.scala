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
package ygg.tests

import quasar.ygg._
import com.precog.common._
import blueeyes._
import scalaz._, Scalaz._
import SampleData._
import TableModule._
import BlockStoreTestModule.module
import ygg.json._

/**
  * This provides an ordering on JValue that mimics how we'd order them as
  * columns in a table, rather than using JValue's default ordering which
  * behaves differently.
  *
  * XXX FIXME
  */

private object JValueInColumnOrder {
  def columnOrder: Ord[JValue] = Ord.order[JValue] { (a, b) =>
    val prims0 = a.flattenWithPath.toMap
    val prims1 = b.flattenWithPath.toMap
    val cols0  = (prims1.mapValues(_ => JUndefined) ++ prims0).toList.sorted
    val cols1  = (prims0.mapValues(_ => JUndefined) ++ prims1).toList.sorted

    cols0 ?|? cols1
  }
}

class BlockAlignSpec extends quasar.Qspec with TableModuleSpec with BlockLoadSpec {
  implicit val order: Ord[JValue] = JValueInColumnOrder.columnOrder

  "align" should {
    "a simple example" in alignSimple
    "across slice boundaries" in alignAcrossBoundaries
    "survive a trivial scalacheck" in checkAlign
    "produce the same results irrespective of input order" in testAlignSymmetry(0)
    "produce the same results irrespective of input order" in testAlignSymmetry(1)
    "produce the same results irrespective of input order" in testAlignSymmetry(2)
  }

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
      "fully homogeneous data" in homogeneousSortSample
      "fully homogeneous data with object" in homogeneousSortSampleWithNonexistentSortKey
      "data with undefined sort keys" in partiallyUndefinedSortSample
      "heterogeneous sort keys" in heterogeneousSortSample
      "heterogeneous sort keys case 2" in heterogeneousSortSample2
      "heterogeneous sort keys ascending" in heterogeneousSortSampleAscending
      "heterogeneous sort keys descending" in heterogeneousSortSampleDescending
      "top-level hetereogeneous values" in heterogeneousBaseValueTypeSample
      "sort with a bad schema" in badSchemaSortSample
      "merges over three cells" in threeCellMerge
      "empty input" in emptySort
      "with uniqueness for keys" in uniqueSort

      "arbitrary datasets" in checkSortDense(SortAscending)
      "arbitrary datasets descending" in checkSortDense(SortDescending)
    }
  }

  def testAlign(sample: SampleData) = {
    import module._
    import module.trans.constants._

    val lstream  = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream  = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }
    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results     <- Table.align(fromJson(lstream), SourceKey.Single, fromJson(rstream), SourceKey.Single)
      leftResult  <- results._1.toJson
      rightResult <- results._2.toJson
      leftResult2 <- results._1.toJson
    } yield {
      (leftResult, rightResult, leftResult2)
    }

    val (leftResult, rightResult, leftResult2) = finalResults.copoint

    ((leftResult must_=== expected)
    && (rightResult must_=== expected)
    && (leftResult must_=== leftResult2))
  }

  def checkAlign = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) =>
      testAlign(sample.sortBy(_ \ "key"))
    }
  }

  def alignSimple = {
    val JArray(elements) = json"""[
        {
          "value":{ "fr8y":-2.761198250953116839E+14037, "hw":[], "q":2.429467767811669098E+50018 },
          "key":[1.0,2.0]
        },
        {
          "value":{ "fr8y":8862932465119160.0, "hw":[], "q":-7.06989214308545856E+34226 },
          "key":[2.0,1.0]
        },
        {
          "value":{ "fr8y":3.754645750547307163E-38452, "hw":[], "q":-2.097582685805979759E+29344 },
          "key":[2.0,4.0]
        },
        {
          "value":{ "fr8y":0.0, "hw":[], "q":2.839669248714535100E+14955 },
          "key":[3.0,4.0]
        },
        {
          "value":{ "fr8y":-1E+8908, "hw":[], "q":6.56825624988914593E-49983 },
          "key":[4.0,2.0]
        },
        {
          "value":{ "fr8y":123473018907070400.0, "hw":[], "q":0E+35485 },
          "key":[4.0,4.0]
        }]
    """.toYgg

    val sample = SampleData(elements.toStream, Some((2, List((JPath(".q"), CNum), (JPath(".hw"), CEmptyArray), (JPath(".fr8y"), CNum)))))

    testAlign(sample.sortBy(_ \ "key"))
  }

  def alignAcrossBoundaries = {
    val JArray(elements) = JParser.parseUnsafe("""[
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[4.0,26.0,21.0]
      },
      {
        "value":8.715632723857159E+307,
        "key":[5.0,39.0,59.0]
      },
      {
        "value":-1.104890528035041E+307,
        "key":[6.0,50.0,5.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.9570651303391037E+307
        },
        "key":[6.0,59.0,9.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.645581770904817E+307
        },
        "key":[9.0,28.0,29.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[10.0,21.0,30.0]
      },
      {
        "value":7.556003912577644E+307,
        "key":[11.0,51.0,15.0]
      },
      {
        "value":3.4877741123656093E+307,
        "key":[11.0,52.0,28.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.263625900450069E+307
        },
        "key":[12.0,25.0,58.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[16.0,31.0,42.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-7.976677386824275E+307
        },
        "key":[21.0,7.0,10.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":5.359853255240687E+307
        },
        "key":[23.0,12.0,26.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":8.988465674311579E+307
        },
        "key":[25.0,39.0,48.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[26.0,13.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.740770480418812E+307
        },
        "key":[27.0,17.0,7.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[27.0,36.0,16.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.988465674311579E+307
        },
        "key":[32.0,13.0,11.0]
      },
      {
        "value":-1.0,
        "key":[32.0,50.0,21.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.0
        },
        "key":[37.0,5.0,43.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":2.911458319070367E+307
        },
        "key":[41.0,17.0,32.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":6.878966108624357E+307
        },
        "key":[48.0,14.0,45.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.024686561894592E+307
        },
        "key":[51.0,26.0,46.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":1.8366746041516641E+307
        },
        "key":[52.0,50.0,54.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-1.0
        },
        "key":[53.0,54.0,27.0]
      },
      {
        "value":{
          "sp7hpv":{ },
          "zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[],
          "xb5hs2ckjajs0k44x":-8.87789254395668E+307
        },
        "key":[55.0,46.0,57.0]
      }]
    """)

    val sample = SampleData(
      elements.toStream,
      Some(
        (3,
         List((JPath(".xb5hs2ckjajs0k44x"), CDouble), (JPath(".zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump"), CEmptyArray), (JPath(".sp7hpv"), CEmptyObject)))))
    testAlign(sample.sortBy(_ \ "key"))
  }

  def testAlignSymmetry(i: Int) = {
    import module._
    import module.trans._

    def test(ltable: Table, alignOnL: TransSpec1, rtable: Table, alignOnR: TransSpec1) = {
      val (ljsondirect, rjsondirect) = (for {
        aligned <- Table.align(ltable, alignOnL, rtable, alignOnR)
        ljson   <- aligned._1.toJson
        rjson   <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      val (ljsonreversed, rjsonreversed) = (for {
        aligned <- Table.align(rtable, alignOnR, ltable, alignOnL)
        ljson   <- aligned._1.toJson
        rjson   <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      (ljsonreversed.toList must_== rjsondirect.toList) and
        (rjsonreversed.toList must_== ljsondirect.toList)
    }

    def test0 = {
      val lsortedOn     = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val rsortedOn     = DerefArrayStatic(Leaf(Source), CPathIndex(1))
      val JArray(ljson) = JParser.parseUnsafe("""[
        [[3],{ "000000":-1 },-1],
        [[4],{ "000000":0 },0],
        [[5],{ "000000":0 },0],
        [[0],{ "000000":1 },1],
        [[2],{ "000000":2126441435 },2126441435],
        [[1],{ "000000":2147483647 },2147483647]
      ]""")

      val JArray(rjson) = JParser.parseUnsafe("""[
        [[1],{ "000000":-2147483648 },-2147483648],
        [[6],{ "000000":-1904025337 },-1904025337],
        [[2],{ "000000":-1456034303 },-1456034303],
        [[4],{ "000000":0 },0],
        [[0],{ "000000":2006322377 },2006322377],
        [[3],{ "000000":2147483647 },2147483647],
        [[5],{ "000000":2147483647 },2147483647]
      ]""")

      test(fromJson(ljson.toStream), lsortedOn, fromJson(rjson.toStream), rsortedOn)
    }

    def test1 = {
      val JArray(ljson) = JParser.parseUnsafe("""[
        [[10],{ "000001":-2, "000000":42 },{ "a":42, "b":-2 }],
        [[7],{ "000001":6, "000000":17 },{ "a":17, "b":6 }],
        [[0],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[5],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[9],{ "000001":12, "000000":21 },{ "a":21, "b":12 }],
        [[13],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[6],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000001":42, "000000":7 },{ "a":7, "b":42 }]
      ]""")

      val lsortedOn = OuterObjectConcat(
        WrapObject(
          DerefObjectStatic(
            OuterObjectConcat(
              WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source), CPathIndex(1)), CPathField("000001")), "000000"),
              WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source), CPathIndex(1)), CPathField("000000")), "000001")
            ),
            CPathField("000000")
          ),
          "000000"
        ))

      val JArray(rjson) = JParser.parseUnsafe("""[
        [[3],{ "000000":1 },{ "b":1 }],
        [[1],{ "000000":6 },{ "b":6 }],
        [[0],{ "000000":12 },{ "b":12 }],
        [[4],{ "000000":12 },{ "b":12 }],
        [[6],{ "000000":42 },{ "b":42 }]
      ]""")

      val rsortedOn = DerefArrayStatic(Leaf(Source), CPathIndex(1))

      test(fromJson(ljson.toStream), lsortedOn, fromJson(rjson.toStream), rsortedOn)
    }

    def test2 = {
      val JArray(ljson)  = JParser.parseUnsafe("""[
        [[6],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000001":42, "000000":7 },{ "a":7, "b":42 }],
        [[7],{ "000001":6, "000000":17 },{ "a":17, "b":6 }],
        [[9],{ "000001":12, "000000":21 },{ "a":21, "b":12 }]
      ]""")
      val JArray(ljson2) = JParser.parseUnsafe("""[
        [[0],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[5],{ "000001":12, "000000":42 },{ "a":42, "b":12 }],
        [[10],{ "000001":-2, "000000":42 },{ "a":42, "b":-2 }],
        [[13],{ "000001":12, "000000":42 },{ "a":42, "b":12 }]
      ]""")

      val lsortedOn = OuterObjectConcat(
        WrapObject(
          DerefObjectStatic(
            OuterObjectConcat(
              WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source), CPathIndex(1)), CPathField("000000")), "000000"),
              WrapObject(DerefObjectStatic(DerefArrayStatic(Leaf(Source), CPathIndex(1)), CPathField("000001")), "000001")
            ),
            CPathField("000000")
          ),
          "000000"
        ))

      val JArray(rjson)  = JParser.parseUnsafe("""[
        [[6],{ "000000":7 },{ "a":7, "b":42 }],
        [[12],{ "000000":7 },{ "a":7 }],
        [[7],{ "000000":17 },{ "a":17, "c":77 }]
      ]""")
      val JArray(rjson2) = JParser.parseUnsafe("""[
        [[0],{ "000000":42 },{ "a":42 }],
        [[1],{ "000000":42 },{ "a":42 }],
        [[13],{ "000000":42 },{ "a":42 }],
        [[2],{ "000000":77 },{ "a":77 }]
      ]""")

      val rsortedOn = DerefArrayStatic(Leaf(Source), CPathIndex(1))

      val ltable = Table(fromJson(ljson.toStream).slices ++ fromJson(ljson2.toStream).slices, UnknownSize)
      val rtable = Table(fromJson(rjson.toStream).slices ++ fromJson(rjson2.toStream).slices, UnknownSize)

      test(ltable, lsortedOn, rtable, rsortedOn)
    }

    i match {
      case 0 => test0
      case 1 => test1
      case 2 => test2
    }
  }

  def testSortDense(sample: SampleData, sortOrder: DesiredSortOrder, unique: Boolean, sortKeys: JPath*) = {
    val jvalueOrdering     = Ord[JValue].toScalaOrdering
    val desiredJValueOrder = if (sortOrder.isAscending) jvalueOrdering else jvalueOrdering.reverse

    val globalIdPath = JPath(".globalId")

    val original = if (unique) {
      sample.data.map { jv =>
        JArray(sortKeys.map(_.extract(jv \ "value")).toList) -> jv
      }.toMap.toList.unzip._2.toStream
    } else {
      sample.data
    }

    // We have to add in and then later remove the global Id (insert
    // order) to match real sort semantics for disambiguation of equal
    // values
    val sorted = original.zipWithIndex.map {
      case (jv, i) => JValue.unsafeInsert(jv, globalIdPath, JNum(i))
    }.sortBy { v =>
      JArray(sortKeys.map(_.extract(v \ "value")).toList ::: List(v \ "globalId")).asInstanceOf[JValue]
    }(desiredJValueOrder).map(_.delete(globalIdPath).get).toList

    val cSortKeys = sortKeys map { CPath(_) }

    val resultM = for {
      sorted <- module.fromSample(sample).sort(module.sortTransspec(cSortKeys: _*), sortOrder)
      json   <- sorted.toJson
    } yield (json, sorted)

    val (result, resultTable) = resultM.copoint

    result.toList must_== sorted

    resultTable.size mustEqual ExactSize(sorted.size)
  }

  def checkSortDense(sortOrder: DesiredSortOrder) = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) =>
      {
        val Some((_, schema)) = sample.schema

        testSortDense(sample, sortOrder, false, schema.map(_._1).head)
      }
    }
  }

  // Simple test of sorting on homogeneous data
  def homogeneousSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "uid":"joe",
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[1]
        },
        {
          "value":{
            "uid":"al",
            "u":false,
            "md":"t",
            "l":[]
          },
          "key":[2]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List(JPath(".uid") -> CString, JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of sorting on homogeneous data with objects
  def homogeneousSortSampleWithNonexistentSortKey = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":6},
        {"key":[1],"value":5}
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List(JPath(".") -> CString))
      )
    )

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of partially undefined sort key data
  def partiallyUndefinedSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "uid":"ted",
            "rzp":{ },
            "hW":1.0,
            "fa":null
          },
          "key":[1]
        },
        {
          "value":{
            "rzp":{ },
            "hW":2.0,
            "fa":null
          },
          "key":[1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List(JPath(".uid") -> CString, JPath(".fa") -> CNull, JPath(".hW") -> CDouble, JPath(".rzp") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"), JPath(".hW"))
  }

  def heterogeneousBaseValueTypeSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value": [0, 1],
          "key":[1]
        },
        {
          "value":{
            "uid": "tom",
            "abc": 2
          },
          "key":[2]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List(JPath("[0]") -> CLong, JPath("[1]") -> CLong, JPath(".uid") -> CString, JPath("abc") -> CLong))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"))
  }

  def badSchemaSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
            "vxu":[],
            "q":-103811160446995821.5,
            "u":5.548109504404496E+307
          },
          "key":[1.0,1.0]
        },
        {
          "value":{
            "vxu":[],
            "q":-8.40213736307813554E+18,
            "u":8.988465674311579E+307
          },
          "key":[1.0,2.0]
        },
        {
          "value":{
            "m":[],
            "f":false
          },
          "key":[2.0,1.0]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some((2, List(JPath(".m") -> CEmptyArray, JPath(".f") -> CBoolean, JPath(".u") -> CDouble, JPath(".q") -> CNum, JPath(".vxu") -> CEmptyArray))))
    testSortDense(sampleData, SortAscending, false, JPath("q"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSample2 = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[1,4,3],"value":{"b0":["",{"alxk":-1},-5.170005125478374E+307],"y":{"pvbT":[-1458654748381439976,{}]}}},
        {"key":[1,4,4],"value":{"y":false,"qvd":[],"aden":{}}},
        {"key":[3,3,3],"value":{"b0":["gxy",{"alxk":-1},6.614267528783459E+307],"y":{"pvbT":[1,{}]}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSampleDescending = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  def heterogeneousSortSampleAscending = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""") --> classOf[JArray]).elements.toStream,
      None)

    testSortDense(sampleData, SortAscending, false, JPath(".y"))
  }

  // Simple test of heterogeneous sort keys
  def heterogeneousSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":{
           "uid": 12,
            "f":{
              "bn":[null],
              "wei":1.0
            },
            "ljz":[null,["W"],true],
            "jmy":4.639428637939817E307
          },
          "key":[1,2,2]
        },
        {
          "value":{
           "uid": 1.5,
            "f":{
              "bn":[null],
              "wei":5.615997508833152E307
            },
            "ljz":[null,[""],false],
            "jmy":-2.612503123965922E307
          },
          "key":[2,1,1]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (3,
         List(
           JPath(".uid")       -> CLong,
           JPath(".uid")       -> CDouble,
           JPath(".f.bn[0]")   -> CNull,
           JPath(".f.wei")     -> CDouble,
           JPath(".ljz[0]")    -> CNull,
           JPath(".ljz[1][0]") -> CString,
           JPath(".ljz[2]")    -> CBoolean,
           JPath(".jmy")       -> CDouble))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"))
  }

  def secondHetSortSample = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":[1.0,0,{

          }],
          "key":[3.0]
        }, {
          "value":{
            "e":null,
            "chl":-1.0,
            "zw1":-4.611686018427387904E-27271
          },
          "key":[1.0]
        }, {
          "value":{
            "e":null,
            "chl":-8.988465674311579E+307,
            "zw1":81740903825956729.9
          },
          "key":[2.0]
        }]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1,
         List(JPath(".e") -> CNull, JPath(".chl") -> CNum, JPath(".zw1") -> CNum, JPath("[0]") -> CLong, JPath("[1]") -> CLong, JPath("[2]") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".zw1"))
  }

  /* The following data set results in three separate JDBM
   * indices due to formats. This exposed a bug in mergeProjections
   * where we weren't properly inverting the cell matrix reorder
   * once one of the index slices expired. See commit
   * a253d47f3f6d09fd39afc2986c529e84e5443e7f for details
   */
  def threeCellMerge = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        {
          "value":-2355162409801206381,
          "key":[1.0,1.0,11.0]
        }, {
          "value":416748368221569769,
          "key":[12.0,10.0,5.0]
        }, {
          "value":1,
          "key":[9.0,13.0,2.0]
        }, {
          "value":4220813543874929309,
          "key":[13.0,10.0,11.0]
        }, {
          "value":{
            "viip":8.988465674311579E+307,
            "ohvhwN":-1.911181119089705905E+11774,
            "zbtQhnpnun":-4364598680493823671
          },
          "key":[8.0,12.0,6.0]
        }, {
          "value":{
            "viip":-8.610170336058498E+307,
            "ohvhwN":0.0,
            "zbtQhnpnun":-3072439692643750408
          },
          "key":[3.0,1.0,12.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":1.255850949484045134E-25873,
            "zbtQhnpnun":-2192537798839555684
          },
          "key":[12.0,10.0,4.0]
        }, {
          "value":{
            "viip":-1.0,
            "ohvhwN":1E-18888,
            "zbtQhnpnun":-1
          },
          "key":[2.0,4.0,11.0]
        }, {
          "value":{
            "viip":1.955487389945603E+307,
            "ohvhwN":-2.220603033978414186E+19,
            "zbtQhnpnun":-1
          },
          "key":[6.0,11.0,5.0]
        }, {
          "value":{
            "viip":-4.022335964233546E+307,
            "ohvhwN":0E+1,
            "zbtQhnpnun":-1
          },
          "key":[8.0,7.0,13.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":-4.611686018427387904E+50018,
            "zbtQhnpnun":0
          },
          "key":[1.0,13.0,12.0]
        }, {
          "value":{
            "viip":0.0,
            "ohvhwN":4.611686018427387903E+26350,
            "zbtQhnpnun":0
          },
          "key":[2.0,7.0,7.0]
        }, {
          "value":{
            "viip":-6.043665565176412E+307,
            "ohvhwN":-4.611686018427387904E+27769,
            "zbtQhnpnun":0
          },
          "key":[2.0,11.0,6.0]
        }, {
          "value":{
            "viip":-1.0,
            "ohvhwN":-1E+36684,
            "zbtQhnpnun":0
          },
          "key":[6.0,4.0,8.0]
        }, {
          "value":{
            "viip":-1.105552122908816E+307,
            "ohvhwN":6.78980055408249814E-41821,
            "zbtQhnpnun":1
          },
          "key":[13.0,6.0,11.0]
        }, {
          "value":{
            "viip":1.0,
            "ohvhwN":3.514965842146513368E-43185,
            "zbtQhnpnun":1133522166006977485
          },
          "key":[13.0,11.0,13.0]
        }, {
          "value":{
            "viip":8.988465674311579E+307,
            "ohvhwN":2.129060503704072469E+45099,
            "zbtQhnpnun":1232928328066014683
          },
          "key":[11.0,3.0,6.0]
        }, {
          "value":{
            "viip":6.651090528711015E+307,
            "ohvhwN":-1.177821034245149979E-49982,
            "zbtQhnpnun":2406980638624125853
          },
          "key":[4.0,5.0,7.0]
        }, {
          "value":{
            "viip":4.648002254349813E+307,
            "ohvhwN":4.611686018427387903E-42682,
            "zbtQhnpnun":2658995085512919727
          },
          "key":[12.0,2.0,8.0]
        }, {
          "value":{
            "viip":0.0,
            "ohvhwN":4.611686018427387903E-33300,
            "zbtQhnpnun":3464601040437655780
          },
          "key":[8.0,10.0,4.0]
        }, {
          "value":{
            "viip":-8.988465674311579E+307,
            "ohvhwN":1E-42830,
            "zbtQhnpnun":3709226396529427859
          },
          "key":[10.0,1.0,4.0]
        }
      ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (3, List(JPath(".zbtQhnpnun") -> CLong, JPath(".ohvhwN") -> CNum, JPath(".viip") -> CNum))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".zbtQhnpnun"))
  }

  def uniqueSort = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[
        { "key" : [2], "value" : { "foo" : 10 } },
        { "key" : [1], "value" : { "foo" : 10 } }
       ]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List())
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }

  def emptySort = {
    val sampleData = SampleData(
      (JParser.parseUnsafe("""[]""") --> classOf[JArray]).elements.toStream,
      Some(
        (1, List())
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }
}
