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
import scalaz._, Scalaz._
import ygg._, common._, json._, table._
import SampleData._

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
    val cols0  = (prims1.mapValues(_ => JUndefined) ++ prims0).toVector.sorted
    val cols1  = (prims0.mapValues(_ => JUndefined) ++ prims1).toVector.sorted

    cols0 ?|? cols1
  }
}

abstract class ProjectionsTableSpec(sampleData: SampleData) extends TableQspec {
  import sampleData.data

  val schema       = sampleData.schema.get._2
  val actualSchema = CValueGenerators inferSchema (data map (_ \ "value"))

  override val projections = List(actualSchema).map { subschema =>
    val stream = data flatMap { jv =>
      val back = subschema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
        case (obj, (jpath, ctype)) => {
          val vpath       = JPath(JPathField("value") :: jpath.nodes)
          val valueAtPath = jv.get(vpath)

          if (CType.compliesWithSchema(valueAtPath, ctype)) {
            obj.set(vpath, valueAtPath)
          } else {
            obj
          }
        }
      }

      if (back \ "value" == JUndefined)
        None
      else
        Some(back)
    }

    Path("/test") -> Projection(stream)
  } toMap

  def testLoadDense() = {
    val expected = data flatMap { jv =>
      val back = schema.foldLeft[JValue](JObject(JField("key", jv \ "key") :: Nil)) {
        case (obj, (jpath, ctype)) => {
          val vpath       = JPath(JPathField("value") :: jpath.nodes)
          val valueAtPath = jv.get(vpath)

          if (CType.compliesWithSchema(valueAtPath, ctype)) {
            obj.set(vpath, valueAtPath)
          } else {
            obj
          }
        }
      }

      (back \ "value" != JUndefined).option(back)
    }

    val cschema = schema map { case (jpath, ctype) => ColumnRef(CPath(jpath), ctype) }
    def ctype  = Schema mkType cschema get
    def result = (this.Table constString Set("/test") load ctype).value.toJson

    result.value.toList must_=== expected.toList
  }
}

class BlockAlignSpec extends TableQspec {
  /** Shadowing the package object order. */
  implicit def JValueOrder: Ord[JValue] = JValueInColumnOrder.columnOrder

  "align" should {
    "a simple example"                                     in alignSimple
    "across slice boundaries"                              in alignAcrossBoundaries
    "survive a trivial scalacheck"                         in checkAlign
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
    }
    "sort" >> {
      "fully homogeneous data"             in homogeneousSortSample
      "fully homogeneous data with object" in homogeneousSortSampleWithNonexistentSortKey
      "data with undefined sort keys"      in partiallyUndefinedSortSample
      "heterogeneous sort keys"            in heterogeneousSortSample
      "heterogeneous sort keys case 2"     in heterogeneousSortSample2
      "heterogeneous sort keys ascending"  in heterogeneousSortSampleAscending
      "heterogeneous sort keys descending" in heterogeneousSortSampleDescending
      "top-level hetereogeneous values"    in heterogeneousBaseValueTypeSample
      "sort with a bad schema"             in badSchemaSortSample
      "merges over three cells"            in threeCellMerge
      "empty input"                        in emptySort
      "with uniqueness for keys"           in uniqueSort
      "arbitrary datasets"                 in checkSortDense(SortAscending).flakyTest
      "arbitrary datasets descending"      in checkSortDense(SortDescending).flakyTest
      "something something het sort"       in secondHetSortSample
    }
  }

  private def testAlign(sample: SampleData) = {
    import trans.constants._

    val lstream  = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 => v }
    val rstream  = sample.data.zipWithIndex collect { case (v, i) if i % 3 == 0 => v }
    val expected = sample.data.zipWithIndex collect { case (v, i) if i % 2 == 0 && i % 3 == 0 => v }

    val finalResults = for {
      results     <- Need(AlignTable(fromJson(lstream), SourceKey.Single, fromJson(rstream), SourceKey.Single))
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

  private def checkAlign = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) =>
      testAlign(sample.sortBy(_ \ "key"))
    }
  }

  private def alignSimple = {
    val data = jsonMany"""
      {"key":[1.0,2.0],"value":{"fr8y":-2.761198250953116839E+14037,"hw":[],"q":2.429467767811669098E+50018}}
      {"key":[2.0,1.0],"value":{"fr8y":8862932465119160.0,"hw":[],"q":-7.06989214308545856E+34226}}
      {"key":[2.0,4.0],"value":{"fr8y":3.754645750547307163E-38452,"hw":[],"q":-2.097582685805979759E+29344}}
      {"key":[3.0,4.0],"value":{"fr8y":0.0,"hw":[],"q":2.839669248714535100E+14955}}
      {"key":[4.0,2.0],"value":{"fr8y":-1E+8908,"hw":[],"q":6.56825624988914593E-49983}}
      {"key":[4.0,4.0],"value":{"fr8y":123473018907070400.0,"hw":[],"q":0E+35485}}
    """

    val sample = SampleData(data.toStream, Some((2, List((JPath(".q"), CNum), (JPath(".hw"), CEmptyArray), (JPath(".fr8y"), CNum)))))
    testAlign(sample.sortBy(_ \ "key"))
  }

  private def alignAcrossBoundaries = {
    val data = jsonMany"""
      {"key":[4.0,26.0,21.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-1.0,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[5.0,39.0,59.0],"value":8.715632723857159E+307}
      {"key":[6.0,50.0,5.0],"value":-1.104890528035041E+307}
      {"key":[6.0,59.0,9.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-1.9570651303391037E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[9.0,28.0,29.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.645581770904817E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[10.0,21.0,30.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":1.0,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[11.0,51.0,15.0],"value":7.556003912577644E+307}
      {"key":[11.0,52.0,28.0],"value":3.4877741123656093E+307}
      {"key":[12.0,25.0,58.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":8.263625900450069E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[16.0,31.0,42.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.988465674311579E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[21.0,7.0,10.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-7.976677386824275E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[23.0,12.0,26.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":5.359853255240687E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[25.0,39.0,48.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":8.988465674311579E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[26.0,13.0,32.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.988465674311579E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[27.0,17.0,7.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":6.740770480418812E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[27.0,36.0,16.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.988465674311579E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[32.0,13.0,11.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.988465674311579E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[32.0,50.0,21.0],"value":-1.0}
      {"key":[37.0,5.0,43.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":1.0,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[41.0,17.0,32.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":2.911458319070367E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[48.0,14.0,45.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":6.878966108624357E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[51.0,26.0,46.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.024686561894592E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[52.0,50.0,54.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":1.8366746041516641E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[53.0,54.0,27.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-1.0,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
      {"key":[55.0,46.0,57.0],"value":{"sp7hpv":{},"xb5hs2ckjajs0k44x":-8.87789254395668E+307,"zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump":[]}}
    """

    val sample = SampleData(
      data.toStream,
      Some(
        (3,
         List((JPath(".xb5hs2ckjajs0k44x"), CDouble), (JPath(".zzTqxfzwzacakwjqeGFcnhpkzd5akfobsg2nxump"), CEmptyArray), (JPath(".sp7hpv"), CEmptyObject)))))
    testAlign(sample.sortBy(_ \ "key"))
  }

  private def testAlignSymmetry(i: Int) = {
    import trans._

    def test(ltable: Table, alignOnL: TransSpec1, rtable: Table, alignOnR: TransSpec1) = {
      val (ljsondirect, rjsondirect) = (for {
        aligned <- Need(AlignTable(ltable, alignOnL, rtable, alignOnR))
        ljson   <- aligned._1.toJson
        rjson   <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      val (ljsonreversed, rjsonreversed) = (for {
        aligned <- Need(AlignTable(rtable, alignOnR, ltable, alignOnL))
        ljson   <- aligned._1.toJson
        rjson   <- aligned._2.toJson
      } yield {
        (ljson, rjson)
      }).copoint

      (ljsonreversed.toList must_== rjsondirect.toList) and
        (rjsonreversed.toList must_== ljsondirect.toList)
    }

    def test0 = {
      val lsortedOn = root(1)
      val rsortedOn = root(1)
      val ljson     = jsonMany"""
        [[3],{"000000":-1},-1]
        [[4],{"000000":0},0]
        [[5],{"000000":0},0]
        [[0],{"000000":1},1]
        [[2],{"000000":2126441435},2126441435]
        [[1],{"000000":2147483647},2147483647]
      """
      val rjson = jsonMany"""
        [[1],{"000000":-2147483648},-2147483648]
        [[6],{"000000":-1904025337},-1904025337]
        [[2],{"000000":-1456034303},-1456034303]
        [[4],{"000000":0},0]
        [[0],{"000000":2006322377},2006322377]
        [[3],{"000000":2147483647},2147483647]
        [[5],{"000000":2147483647},2147483647]
      """

      test(fromJson(ljson.toStream), lsortedOn, fromJson(rjson.toStream), rsortedOn)
    }

    def test1 = {
      val ljson = jsonMany"""
        [[10],{"000000":42,"000001":-2},{"a":42,"b":-2}]
        [[7],{"000000":17,"000001":6},{"a":17,"b":6}]
        [[0],{"000000":42,"000001":12},{"a":42,"b":12}]
        [[5],{"000000":42,"000001":12},{"a":42,"b":12}]
        [[9],{"000000":21,"000001":12},{"a":21,"b":12}]
        [[13],{"000000":42,"000001":12},{"a":42,"b":12}]
        [[6],{"000000":7,"000001":42},{"a":7,"b":42}]
        [[12],{"000000":7,"000001":42},{"a":7,"b":42}]
      """
      val rjson = jsonMany"""
        [[3],{ "000000":1 },{ "b":1 }]
        [[1],{ "000000":6 },{ "b":6 }]
        [[0],{ "000000":12 },{ "b":12 }]
        [[4],{ "000000":12 },{ "b":12 }]
        [[6],{ "000000":42 },{ "b":42 }]
      """

      val lsortedOn = OuterObjectConcat(
        WrapObject(
          DerefObjectStatic(
            OuterObjectConcat(
              WrapObject(root(1).`000001`, "000000"),
              WrapObject(root(1).`000000`, "000001")
            ),
            CPathField("000000")
          ),
          "000000"
        )
      )

      val rsortedOn = root(1)
      test(fromJson(ljson), lsortedOn, fromJson(rjson), rsortedOn)
    }

    def test2 = {
      val ljson = jsonMany"""
        [[6],{ "000001":42, "000000":7 },{ "a":7, "b":42 }]
        [[12],{ "000001":42, "000000":7 },{ "a":7, "b":42 }]
        [[7],{ "000001":6, "000000":17 },{ "a":17, "b":6 }]
        [[9],{ "000001":12, "000000":21 },{ "a":21, "b":12 }]
      """
      val ljson2 = jsonMany"""
        [[0],{ "000001":12, "000000":42 },{ "a":42, "b":12 }]
        [[5],{ "000001":12, "000000":42 },{ "a":42, "b":12 }]
        [[10],{ "000001":-2, "000000":42 },{ "a":42, "b":-2 }]
        [[13],{ "000001":12, "000000":42 },{ "a":42, "b":12 }]
      """
      val rjson  = jsonMany"""
        [[6],{ "000000":7 },{ "a":7, "b":42 }]
        [[12],{ "000000":7 },{ "a":7 }]
        [[7],{ "000000":17 },{ "a":17, "c":77 }]
      """
      val rjson2 = jsonMany"""
        [[0],{ "000000":42 },{ "a":42 }]
        [[1],{ "000000":42 },{ "a":42 }]
        [[13],{ "000000":42 },{ "a":42 }]
        [[2],{ "000000":77 },{ "a":77 }]
      """

      val lsortedOn = OuterObjectConcat(
        WrapObject(
          DerefObjectStatic(
            OuterObjectConcat(
              WrapObject(root(1).`000000`, "000000"),
              WrapObject(root(1).`000001`, "000001")
            ),
            CPathField("000000")
          ),
          "000000"
        ))


      val rsortedOn = root(1)
      val ltable    = Table(fromJson(ljson).slices ++ fromJson(ljson2).slices, UnknownSize)
      val rtable    = Table(fromJson(rjson).slices ++ fromJson(rjson2).slices, UnknownSize)

      test(ltable, lsortedOn, rtable, rsortedOn)
    }

    i match {
      case 0 => test0
      case 1 => test1
      case 2 => test2
    }
  }

  private def testSortDense(sample: SampleData, sortOrder: DesiredSortOrder, unique: Boolean, sortKeys: JPath*) = {
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
      case (jv, i) => jv.unsafeInsert(globalIdPath, JNum(i))
    }.sortBy { v =>
      JArray(sortKeys.map(_.extract(v \ "value")).toList ::: List(v \ "globalId")).asInstanceOf[JValue]
    }(desiredJValueOrder).map(_.delete(globalIdPath).get).toList

    val cSortKeys = sortKeys map { CPath(_) }

    val resultM = for {
      sorted <- fromSample(sample).sort(sortTransspec(cSortKeys: _*), sortOrder)
      json   <- sorted.toJson
    } yield (json, sorted)

    val (result, resultTable) = resultM.copoint

    result.toList must_== sorted

    resultTable.size mustEqual ExactSize(sorted.size)
  }

  private def checkSortDense(sortOrder: DesiredSortOrder) = {
    implicit val gen = sample(objectSchema(_, 3))
    prop { (sample: SampleData) =>
      {
        val Some((_, schema)) = sample.schema

        testSortDense(sample, sortOrder, false, schema.map(_._1).head)
      }
    }
  }

  // Simple test of sorting on homogeneous data
  private def homogeneousSortSample = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[1],"value":{"l":[],"md":"t","u":false,"uid":"joe"}}
        {"key":[2],"value":{"l":[],"md":"t","u":false,"uid":"al"}}
      """.toStream,
      Some(
        (1, List(JPath(".uid") -> CString, JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of sorting on homogeneous data with objects
  private def homogeneousSortSampleWithNonexistentSortKey = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[2],"value":6}
        {"key":[1],"value":5}
      """.toStream,
      Some(
        (1, List(JPath(".") -> CString))
      )
    )

    testSortDense(sampleData, SortDescending, false, JPath(".uid"))
  }

  // Simple test of partially undefined sort key data
  private def partiallyUndefinedSortSample = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[1],"value":{"fa":null,"hW":1.0,"rzp":{},"uid":"ted"}}
        {"key":[1],"value":{"fa":null,"hW":2.0,"rzp":{}}}
      """.toStream,
      Some(
        (1, List(JPath(".uid") -> CString, JPath(".fa") -> CNull, JPath(".hW") -> CDouble, JPath(".rzp") -> CEmptyObject))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"), JPath(".hW"))
  }

  private def heterogeneousBaseValueTypeSample = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[1],"value":[0,1]}
        {"key":[2],"value":{"abc":2,"uid":"tom"}}
      """.toStream,
      Some(
        (1, List(JPath("[0]") -> CLong, JPath("[1]") -> CLong, JPath(".uid") -> CString, JPath("abc") -> CLong))
      )
    )

    testSortDense(sampleData, SortAscending, false, JPath(".uid"))
  }

  private def badSchemaSortSample = {
    val data = jsonMany"""
      {"key":[1.0,1.0],"value":{"q":-103811160446995821.5,"u":5.548109504404496E+307,"vxu":[]}}
      {"key":[1.0,2.0],"value":{"q":-8.40213736307813554E+18,"u":8.988465674311579E+307,"vxu":[]}}
      {"key":[2.0,1.0],"value":{"f":false,"m":[]}}
    """
    val sampleData = SampleData(
      data.toStream,
      Some((2, List(JPath(".m") -> CEmptyArray, JPath(".f") -> CBoolean, JPath(".u") -> CDouble, JPath(".q") -> CNum, JPath(".vxu") -> CEmptyArray))))
    testSortDense(sampleData, SortAscending, false, JPath("q"))
  }

  // Simple test of sorting on heterogeneous data
  private def heterogeneousSortSample2 = {
    val sampleData = SampleData(
      (json"""[
        {"key":[1,4,3],"value":{"b0":["",{"alxk":-1},-5.170005125478374E+307],"y":{"pvbT":[-1458654748381439976,{}]}}},
        {"key":[1,4,4],"value":{"y":false,"qvd":[],"aden":{}}},
        {"key":[3,3,3],"value":{"b0":["gxy",{"alxk":-1},6.614267528783459E+307],"y":{"pvbT":[1,{}]}}}
      ]""".asArray).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  private def heterogeneousSortSampleDescending = {
    val sampleData = SampleData(
      (json"""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""".asArray).elements.toStream,
      None)

    testSortDense(sampleData, SortDescending, false, JPath(".y"))
  }

  // Simple test of sorting on heterogeneous data
  private def heterogeneousSortSampleAscending = {
    val sampleData = SampleData(
      (json"""[
        {"key":[2],"value":{"y":false}},
        {"key":[3],"value":{"y":{"pvbT":1}}}
      ]""".asArray).elements.toStream,
      None)

    testSortDense(sampleData, SortAscending, false, JPath(".y"))
  }

  // Simple test of heterogeneous sort keys
  private def heterogeneousSortSample = {
    val data = jsonMany"""
      {"key":[1,2,2],"value":{"f":{"bn":[null],"wei":1.0},"jmy":4.639428637939817E+307,"ljz":[null,["W"],true],"uid":12}}
      {"key":[2,1,1],"value":{"f":{"bn":[null],"wei":5.615997508833152E+307},"jmy":-2.612503123965922E+307,"ljz":[null,[""],false],"uid":1.5}}
    """

    val sampleData = SampleData(
      data.toStream,
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

  private def secondHetSortSample = {
    val data = jsonMany"""
      {"key":[3.0],"value":[1.0,0,{}]}
      {"key":[1.0],"value":{"chl":-1.0,"e":null,"zw1":-4.611686018427387904E-27271}}
      {"key":[2.0],"value":{"chl":-8.988465674311579E+307,"e":null,"zw1":81740903825956729.9}}
    """

    val sampleData = SampleData(
      data.toStream,
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
  private def threeCellMerge = {
    val data = jsonMany"""
      {"key":[1.0,1.0,11.0],"value":-2355162409801206381}
      {"key":[12.0,10.0,5.0],"value":416748368221569769}
      {"key":[9.0,13.0,2.0],"value":1}
      {"key":[13.0,10.0,11.0],"value":4220813543874929309}
      {"key":[8.0,12.0,6.0],"value":{"ohvhwN":-1.911181119089705905E+11774,"viip":8.988465674311579E+307,"zbtQhnpnun":-4364598680493823671}}
      {"key":[3.0,1.0,12.0],"value":{"ohvhwN":0.0,"viip":-8.610170336058498E+307,"zbtQhnpnun":-3072439692643750408}}
      {"key":[12.0,10.0,4.0],"value":{"ohvhwN":1.255850949484045134E-25873,"viip":1.0,"zbtQhnpnun":-2192537798839555684}}
      {"key":[2.0,4.0,11.0],"value":{"ohvhwN":1E-18888,"viip":-1.0,"zbtQhnpnun":-1}}
      {"key":[6.0,11.0,5.0],"value":{"ohvhwN":-2.220603033978414186E+19,"viip":1.955487389945603E+307,"zbtQhnpnun":-1}}
      {"key":[8.0,7.0,13.0],"value":{"ohvhwN":0E+1,"viip":-4.022335964233546E+307,"zbtQhnpnun":-1}}
      {"key":[1.0,13.0,12.0],"value":{"ohvhwN":-4.611686018427387904E+50018,"viip":1.0,"zbtQhnpnun":0}}
      {"key":[2.0,7.0,7.0],"value":{"ohvhwN":4.611686018427387903E+26350,"viip":0.0,"zbtQhnpnun":0}}
      {"key":[2.0,11.0,6.0],"value":{"ohvhwN":-4.611686018427387904E+27769,"viip":-6.043665565176412E+307,"zbtQhnpnun":0}}
      {"key":[6.0,4.0,8.0],"value":{"ohvhwN":-1E+36684,"viip":-1.0,"zbtQhnpnun":0}}
      {"key":[13.0,6.0,11.0],"value":{"ohvhwN":6.78980055408249814E-41821,"viip":-1.105552122908816E+307,"zbtQhnpnun":1}}
      {"key":[13.0,11.0,13.0],"value":{"ohvhwN":3.514965842146513368E-43185,"viip":1.0,"zbtQhnpnun":1133522166006977485}}
      {"key":[11.0,3.0,6.0],"value":{"ohvhwN":2.129060503704072469E+45099,"viip":8.988465674311579E+307,"zbtQhnpnun":1232928328066014683}}
      {"key":[4.0,5.0,7.0],"value":{"ohvhwN":-1.177821034245149979E-49982,"viip":6.651090528711015E+307,"zbtQhnpnun":2406980638624125853}}
      {"key":[12.0,2.0,8.0],"value":{"ohvhwN":4.611686018427387903E-42682,"viip":4.648002254349813E+307,"zbtQhnpnun":2658995085512919727}}
      {"key":[8.0,10.0,4.0],"value":{"ohvhwN":4.611686018427387903E-33300,"viip":0.0,"zbtQhnpnun":3464601040437655780}}
      {"key":[10.0,1.0,4.0],"value":{"ohvhwN":1E-42830,"viip":-8.988465674311579E+307,"zbtQhnpnun":3709226396529427859}}
    """

    val sampleData = SampleData(
      data.toStream,
      Some((3, List(JPath(".zbtQhnpnun") -> CLong, JPath(".ohvhwN") -> CNum, JPath(".viip") -> CNum)))
    )

    testSortDense(sampleData, SortAscending, false, JPath(".zbtQhnpnun"))
  }

  private def uniqueSort = {
    val sampleData = SampleData(
      jsonMany"""
        { "key" : [2], "value" : { "foo" : 10 } }
        { "key" : [1], "value" : { "foo" : 10 } }
      """.toStream,
      Some(1 -> Nil)
    )

    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }

  private def emptySort = {
    val sampleData = SampleData(Stream(), Some(1 -> Nil))
    testSortDense(sampleData, SortAscending, false, JPath(".foo"))
  }

  private def testLoadDense(sample: SampleData) = (new ProjectionsTableSpec(sample) {}).testLoadDense()

  private def testLoadSample1 = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[1],"value":{"l":[],"md":"t","u":false}}
      """.toStream,
      Some(
        (1, List(JPath(".u") -> CBoolean, JPath(".md") -> CString, JPath(".l") -> CEmptyArray))
      )
    )

    testLoadDense(sampleData)
  }

  private def testLoadSample2 = {
    testLoadDense(
      SampleData(
        jsonMany"""{"key":[2,1],"value":{"fa":null,"hW":1.0,"rzp":{}}}""".toStream,
        Some((2, List(JPath(".fa") -> CNull, JPath(".hW") -> CLong, JPath(".rzp") -> CEmptyObject)))
      )
    )
  }

  private def testLoadSample3 = {
    val sampleData = SampleData(
      jsonMany"""
        {"key":[1,2,2],"value":{"f":{"bn":[null],"wei":1.0},"jmy":4.639428637939817E+307,"ljz":[null,["W"],true]}}
        {"key":[2,1,1],"value":{"f":{"bn":[null],"wei":5.615997508833152E+307},"jmy":-2.612503123965922E+307,"ljz":[null,[""],false]}}
      """.toStream,
      Some(
        (3,
         List(
           JPath(".f.bn[0]")   -> CNull,
           JPath(".f.wei")     -> CLong,
           JPath(".f.wei")     -> CDouble,
           JPath(".ljz[0]")    -> CNull,
           JPath(".ljz[1][0]") -> CString,
           JPath(".ljz[2]")    -> CBoolean,
           JPath(".jmy")       -> CDouble))
      )
    )

    testLoadDense(sampleData)
  }

  private def testLoadSample4 = {
    val data = jsonMany"""
      {"key":[1,1],"value":{"dV":{"d":true,"l":false,"vq":{}},"oy":{"nm":false},"uR":-6.41847178802919E+307}}
    """
    val sampleData = SampleData(
      data.toStream,
      Some(
        (2,
         List(JPath(".dV.d") -> CBoolean, JPath(".dV.l") -> CBoolean, JPath(".dV.vq") -> CEmptyObject, JPath(".oy.nm") -> CBoolean, JPath(".uR") -> CDouble))
      )
    )

    testLoadDense(sampleData)
  }

  private def sortTransspec(sortKeys: CPath*): TransSpec1 = {
    import trans._
    InnerObjectConcat(sortKeys.zipWithIndex.map {
      case (sortKey, idx) =>
        WrapObject(
          sortKey.nodes.foldLeft[TransSpec1](DerefObjectStatic(Leaf(Source), CPathField("value"))) {
            case (innerSpec, field: CPathField) => DerefObjectStatic(innerSpec, field)
            case (innerSpec, index: CPathIndex) => DerefArrayStatic(innerSpec, index)
            case x                              => abort(s"Unexpected arg $x")
          },
          "%09d".format(idx)
        )
    }: _*)
  }
}
