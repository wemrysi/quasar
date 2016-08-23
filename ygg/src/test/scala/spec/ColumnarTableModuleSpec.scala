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

import ygg.table._
import ygg.common._
import scalaz._, Scalaz._
import TableModule._
import SampleData._
import ygg.json._

class ColumnarTableModuleSpec
      extends ColumnarTableQspec
         with TableModuleSpec
         with CogroupSpec
         with CrossSpec
         with TransformSpec
         with CompactSpec
         with TakeRangeSpec
         with CanonicalizeSpec
         with PartitionMergeSpec
         with ToArraySpec
         with SampleSpec
         with DistinctSpec
         with SchemasSpec {

  import trans._

  class Table(slices: StreamT[Need, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    import trans._

    def load(apiKey: APIKey, jtpe: JType)                                                                                           = ???
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean)                                                     = Need(this)
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Seq[Table]] = ???
  }

  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: StreamT[Need, Slice], size: TableSize) = new Table(slices, size)

    def singleton(slice: Slice) = new Table(slice :: StreamT.empty[Need, Slice], ExactSize(1))

    def align(sourceLeft: Table, alignOnL: TransSpec1, sourceRight: Table, alignOnR: TransSpec1): Need[Table -> Table] =
      abort("not implemented here")
  }

  object Table extends TableCompanion

  def testConcat = {
    val data1: Stream[JValue] = Stream.fill(25)(json"""{ "a": 1, "b": "x", "c": null }""")
    val data2: Stream[JValue] = Stream.fill(35)(json"""[4, "foo", null, true]""")

    val table1   = fromSample(SampleData(data1), Some(10))
    val table2   = fromSample(SampleData(data2), Some(10))
    val results  = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.copoint must_== expected
  }

  def streamToString(stream: StreamT[Need, CharBuffer]): String = {
    def loop(stream: StreamT[Need, CharBuffer], sb: StringBuilder): Need[String] =
      stream.uncons.flatMap {
        case None =>
          Need(sb.toString)
        case Some((cb, tail)) =>
          sb.append(cb)
          loop(tail, sb)
      }
    loop(stream, new StringBuilder).copoint
  }


  def testRenderCsv(json: String, maxSliceSize: Option[Int] = None): String = {
    val es    = JParser.parseManyFromString(json).valueOr(throw _)
    val table = fromJson(es.toStream, maxSliceSize)
    streamToString(table.renderCsv())
  }

  def testRenderJson(xs: JValue*) = {
    val seq = xs.toVector
    def minimizeItem(t: (String, JValue)) = minimize(t._2).map((t._1, _))

    def minimize(value: JValue): Option[JValue] = value match {
      case JUndefined       => None
      case JObject(fields)  => Some(JObject(fields.flatMap(minimizeItem)))
      case JArray(Seq())    => Some(jarray())
      case JArray(elements) => elements flatMap minimize match { case Seq() => None ; case xs => Some(JArray(xs)) }
      case v                => Some(v)
    }

    val table     = fromJson(seq.toStream)
    val expected  = JArray(seq.toVector)
    val arrayM    = table.renderJson("[", ",", "]").foldLeft("")(_ + _.toString).map(JParser.parseUnsafe)
    val minimized = minimize(expected) getOrElse jarray()

    arrayM.copoint mustEqual minimized
  }

  def sanitize(s: String): String = s.toArray.map(c => if (c < ' ') ' ' else c).mkString("")
  def undef: JValue = JUndefined

  def renderLotsToCsv(lots: Int, maxSliceSize: Option[Int] = None) = {
    val event    = "{\"x\":123,\"y\":\"foobar\",\"z\":{\"xx\":1.0,\"yy\":2.0}}"
    val events   = event * lots
    val csv      = testRenderCsv(events, maxSliceSize)
    val expected = ".x,.y,.z.xx,.z.yy\r\n" + ("123,foobar,1,2\r\n" * lots)
    csv must_=== expected
  }

  "a table dataset" should {
    "verify bijection from static JSON" in {
      val sample = jsonMany"""
        {"key":[-1,0],"value":null}
        {"key":[-3090012080927607325,2875286661755661474],"value":{"lwu":-5.121099465699862E+307,"q8b":[6.615224799778253E+307,[false,null,-8.988465674311579E+307,-3.536399224770604E+307]]}}
        {"key":[-3918416808128018609,-1],"value":-1.0}
      """

      val dataset = fromJson(sample)
      val results = dataset.toJson

      (results.copoint.toSeq: Seq[JValue]) must_=== sample
    }

    "verify bijection from JSON" in checkMappings(this)

    "verify renderJson round tripping" in {
      implicit val gen = sample(schema)
      prop((sd: SampleData) => testRenderJson(sd.data: _*))
    }.set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)

    "handle special cases of renderJson" >> {

      "undefined at beginning of array"  >> testRenderJson(jarray(undef, JNum(1), JNum(2)))
      "undefined in middle of array"     >> testRenderJson(jarray(JNum(1), undef, JNum(2)))
      "fully undefined array"            >> testRenderJson(jarray(undef, undef, undef))
      "undefined at beginning of object" >> testRenderJson(jobject("foo" -> undef, "bar" -> JNum(1), "baz" -> JNum(2)))
      "undefined in middle of object"    >> testRenderJson(jobject("foo" -> JNum(1), "bar" -> undef, "baz" -> JNum(2)))
      "fully undefined object"           >> testRenderJson(jobject())
      "undefined row"                    >> testRenderJson(jobject(), JNum(42))

      "check utf-8 encoding" in prop((s: String) => testRenderJson(json"${ sanitize(s) }")).set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)
      "check long encoding"  in prop((x: Long) => testRenderJson(json"$x")).set(minTestsOk = 20000, workers = Runtime.getRuntime.availableProcessors)
    }

    "in cogroup" >> {
      "perform a trivial cogroup"                                                in testTrivialCogroup(identity[Table])
      "perform a simple cogroup"                                                 in testSimpleCogroup(identity[Table])
      "perform another simple cogroup"                                           in testAnotherSimpleCogroup
      "cogroup for unions"                                                       in testUnionCogroup
      "perform yet another simple cogroup"                                       in testAnotherSimpleCogroupSwitched
      "cogroup across slice boundaries"                                          in testCogroupSliceBoundaries
      "error on unsorted inputs"                                                 in testUnsortedInputs
      "cogroup partially defined inputs properly"                                in testPartialUndefinedCogroup

      "survive pathology 1"                                                      in testCogroupPathology1
      "survive pathology 2"                                                      in testCogroupPathology2
      "survive pathology 3"                                                      in testCogroupPathology3

      "not truncate cogroup when right side has long equal spans"                in testLongEqualSpansOnRight
      "not truncate cogroup when left side has long equal spans"                 in testLongEqualSpansOnLeft
      "not truncate cogroup when both sides have long equal spans"               in testLongEqualSpansOnBoth
      "not truncate cogroup when left side is long span and right is increasing" in testLongLeftSpanWithIncreasingRight

      "survive scalacheck" in {
        prop { cogroupData: (SampleData, SampleData) => testCogroup(cogroupData._1, cogroupData._2) }
      }
    }

    "in cross" >> {
      "perform a simple cartesian" in testSimpleCross

      "split a cross that would exceed maxSliceSize boundaries" in {
        val sample = jsonMany"""
          {"key":[-1,0],"value":null}
          {"key":[-3090012080927607325,2875286661755661474],"value":{"lwu":-5.121099465699862E+307,"q8b":[6.615224799778253E+307,[false,null,-8.988465674311579E+307],-3.536399224770604E+307]}}
          {"key":[-3918416808128018609,-1],"value":-1.0}
          {"key":[-3918416898128018609,-2],"value":-1.0}
          {"key":[-3918426808128018609,-3],"value":-1.0}
        """

        val dataset1 = fromJson(sample, Some(3))

        dataset1.cross(dataset1)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight))).slices.uncons.copoint must beLike {
          case Some((head, _)) => head.size must beLessThanOrEqualTo(yggConfig.maxSliceSize)
        }
      }

      "cross across slice boundaries on one side" in testCrossSingles
      "survive scalacheck" in {
        prop { cogroupData: (SampleData, SampleData) => testCross(cogroupData._1, cogroupData._2) }
      }
    }

    "in transform" >> {
      "perform the identity transform"                                          in checkTransformLeaf

      "perform a trivial map1"                                                  in testMap1IntLeaf
      "perform deepmap1 using numeric coercion"                                 in testDeepMap1CoerceToDouble
      "perform map1 using numeric coercion"                                     in testMap1CoerceToDouble
      "fail to map1 into array and object"                                      in testMap1ArrayObject
      "perform a less trivial map1"                                             in checkMap1.pendingUntilFixed

      "give the identity transform for the trivial 'true' filter"               in checkTrueFilter
      "give the identity transform for a nontrivial filter"                     in checkFilter.pendingUntilFixed
      "give a transformation for a big decimal and a long"                      in testMod2Filter

      "perform an object dereference"                                           in checkObjectDeref
      "perform an array dereference"                                            in checkArrayDeref
      "perform metadata dereference on data without metadata"                   in checkMetaDeref

      "perform a trivial map2 add"                                              in checkMap2Add.pendingUntilFixed
      "perform a trivial map2 eq"                                               in checkMap2Eq
      "perform a map2 add over but not into arrays and objects"                 in testMap2ArrayObject

      "perform a trivial equality check"                                        in checkEqualSelf
      "perform a trivial equality check on an array"                            in checkEqualSelfArray
      "perform a slightly less trivial equality check"                          in checkEqual
      "test a failing equality example"                                         in testEqual1
      "perform a simple equality check"                                         in testSimpleEqual
      "perform another simple equality check"                                   in testAnotherSimpleEqual
      "perform yet another simple equality check"                               in testYetAnotherSimpleEqual
      "perform a simple not-equal check"                                        in testASimpleNonEqual

      "perform a equal-literal check"                                           in checkEqualLiteral
      "perform a not-equal-literal check"                                       in checkNotEqualLiteral

      "wrap the results of a transform inside an object as the specified field" in checkWrapObject
      "give the identity transform for self-object concatenation"               in checkObjectConcatSelf
      "use a right-biased overwrite strategy when object concat conflicts"      in checkObjectConcatOverwrite
      "test inner object concat with a single boolean"                          in testObjectConcatSingletonNonObject
      "test inner object concat with a boolean and an empty object"             in testObjectConcatTrivial
      "concatenate dissimilar objects"                                          in checkObjectConcat
      "test inner object concat join semantics"                                 in testInnerObjectConcatJoinSemantics
      "test inner object concat with empty objects"                             in testInnerObjectConcatEmptyObject
      "test outer object concat with empty objects"                             in testOuterObjectConcatEmptyObject
      "test inner object concat with undefined"                                 in testInnerObjectConcatUndefined
      "test outer object concat with undefined"                                 in testOuterObjectConcatUndefined
      "test inner object concat with empty"                                     in testInnerObjectConcatLeftEmpty
      "test outer object concat with empty"                                     in testOuterObjectConcatLeftEmpty

      "concatenate dissimilar arrays"                                           in checkArrayConcat
      "inner concatenate arrays with undefineds"                                in testInnerArrayConcatUndefined
      "outer concatenate arrays with undefineds"                                in testOuterArrayConcatUndefined
      "inner concatenate arrays with empty arrays"                              in testInnerArrayConcatEmptyArray
      "outer concatenate arrays with empty arrays"                              in testOuterArrayConcatEmptyArray
      "inner array concatenate when one side is not an array"                   in testInnerArrayConcatLeftEmpty
      "outer array concatenate when one side is not an array"                   in testOuterArrayConcatLeftEmpty

      "delete elements according to a JType" in checkObjectDelete
      "delete only field in object without removing from array" in {
        val JArray(elements) = json"""[
          {"foo": 4, "bar": 12},
          {"foo": 5},
          {"bar": 45},
          {},
          {"foo": 7, "bar" :23, "baz": 24}
        ]"""

        val sample   = SampleData(elements.toStream)
        val table    = fromSample(sample)
        val spec     = ObjectDelete(Leaf(Source), Set(CPathField("foo")))
        val results  = toJson(table.transform(spec))
        val expected = jsonMany"""
          {"bar": 12}
          {}
          {"bar": 45}
          {}
          {"bar" :23, "baz": 24}
        """
        (results.copoint: Seq[JValue]) must_=== expected
      }

      "perform a basic IsType transformation"                    in testIsTypeTrivial
      "perform an IsType transformation on numerics"             in testIsTypeNumeric
      "perform an IsType transformation on trivial union"        in testIsTypeUnionTrivial
      "perform an IsType transformation on union"                in testIsTypeUnion
      "perform an IsType transformation on nested unfixed types" in testIsTypeUnfixed
      "perform an IsType transformation on objects"              in testIsTypeObject
      "perform an IsType transformation on unfixed objects"      in testIsTypeObjectUnfixed
      "perform an IsType transformation on unfixed arrays"       in testIsTypeArrayUnfixed
      "perform an IsType transformation on empty objects"        in testIsTypeObjectEmpty
      "perform an IsType transformation on empty arrays"         in testIsTypeArrayEmpty
      "perform a check on IsType"                                in checkIsType

      "perform a trivial type-based filter"                      in checkTypedTrivial
      "perform a less trivial type-based filter"                 in checkTyped
      "perform a type-based filter across slice boundaries"      in testTypedAtSliceBoundary
      "perform a trivial heterogeneous type-based filter"        in testTypedHeterogeneous
      "perform a trivial object type-based filter"               in testTypedObject
      "retain all object members when typed to unfixed object"   in testTypedObjectUnfixed
      "perform another trivial object type-based filter"         in testTypedObject2
      "perform a trivial array type-based filter"                in testTypedArray
      "perform another trivial array type-based filter"          in testTypedArray2
      "perform yet another trivial array type-based filter"      in testTypedArray3
      "perform a fourth trivial array type-based filter"         in testTypedArray4
      "perform a trivial number type-based filter"               in testTypedNumber
      "perform another trivial number type-based filter"         in testTypedNumber2
      "perform a filter returning the empty set"                 in testTypedEmpty

      "perform a summation scan case 1"                          in testTrivialScan
      "perform a summation scan of heterogeneous data"           in testHetScan
      "perform a summation scan"                                 in checkScan
      "perform dynamic object deref"                             in testDerefObjectDynamic
      "perform an array swap"                                    in checkArraySwap
      "replace defined rows with a constant"                     in checkConst

      "check cond" in checkCond.pendingUntilFixed
    }

    "in compact" >> {
      "be the identity on fully defined tables"  in testCompactIdentity
      "preserve all defined rows"                in testCompactPreserve
      "have no undefined rows"                   in testCompactRows
      "have no empty slices"                     in testCompactSlices
      "preserve all defined key rows"            in testCompactPreserveKey
      "have no undefined key rows"               in testCompactRowsKey
      "have no empty key slices"                 in testCompactSlicesKey
    }

    "in distinct" >> {
      "be the identity on tables with no duplicate rows"                            in testDistinctIdentity
      "peform properly when the same row appears inside two different slices"       in testDistinctAcrossSlices
      "peform properly again when the same row appears inside two different slices" in testDistinctAcrossSlices2
      "have no duplicate rows"                                                      in testDistinct
    }

    "in takeRange" >> {
      "select the correct rows: trivial case"                                               in testTakeRange
      "select the correct rows when we take past the end of the table"                      in testTakeRangeLarger
      "select the correct rows when we start at an index larger than the size of the table" in testTakeRangeEmpty
      "select the correct rows across slice boundary"                                       in testTakeRangeAcrossSlices
      "select the correct rows: second slice"                                               in testTakeRangeSecondSlice
      "select the first slice"                                                              in testTakeRangeFirstSliceOnly
      "select nothing with a negative starting index"                                       in testTakeRangeNegStart
      "select nothing with a negative number to take"                                       in testTakeRangeNegNumber
      "select the correct rows using scalacheck"                                            in checkTakeRange
    }

    "in toArray" >> {
      "create a single column given two single columns" in testToArrayHomogeneous
      "create a single column given heterogeneous data" in testToArrayHeterogeneous
    }

    "in concat" >> {
      "concat two tables" in testConcat
    }

    "in canonicalize" >> {
      "return the correct slice sizes using scalacheck" in checkCanonicalize
      "return the slice size in correct bound using scalacheck with range" in checkBoundedCanonicalize
      "return the correct slice sizes in a trivial case" in testCanonicalize
      "return the correct slice sizes given length zero" in testCanonicalizeZero
      "return the correct slice sizes along slice boundaries" in testCanonicalizeBoundary
      "return the correct slice sizes greater than slice boundaries" in testCanonicalizeOverBoundary
      "return empty table when given empty table" in testCanonicalizeEmpty
      "remove slices of size zero" in testCanonicalizeEmptySlices
    }

    "in schemas" >> {
      "find a schema in single-schema table" in testSingleSchema
      "find a schema in homogeneous array table" in testHomogeneousArraySchema
      "find schemas separated by slice boundary" in testCrossSliceSchema
      "extract intervleaved schemas" in testIntervleavedSchema
      "don't include undefineds in schema" in testUndefinedsInSchema
      "deal with most expected types" in testAllTypesInSchema
    }

    "in sample" >> {
       "sample from a dataset" in testSample
       "return no samples given empty sequence of transspecs" in testSampleEmpty
       "sample from a dataset given non-identity transspecs" in testSampleTransSpecs
       "return full set when sample size larger than dataset" in testLargeSampleSize
       "resurn empty table when sample size is 0" in test0SampleSize
    }
  }

  "partitionMerge" should {
    "concatenate reductions of subsequences" in testPartitionMerge
  }

  "track table metrics" should {
    "single traversal" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / yggConfig.maxSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple transforms" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / yggConfig.maxSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.transform(TransSpec1.Id).transform(TransSpec1.Id).transform(TransSpec1.Id)
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 1
        table.metrics.sliceTraversedCount must_== expectedSlices
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "multiple forcing calls" >> {
      implicit val gen = sample(objectSchema(_, 3))
      prop { (sample: SampleData) =>
        val expectedSlices = (sample.data.size.toDouble / yggConfig.maxSliceSize).ceil

        val table = fromSample(sample)
        val t0 = table.compact(TransSpec1.Id).compact(TransSpec1.Id).compact(TransSpec1.Id)
        table.toJson.copoint must_== sample.data
        t0.toJson.copoint must_== sample.data

        table.metrics.startCount must_== 2
        table.metrics.sliceTraversedCount must_== (expectedSlices * 2)
        t0.metrics.startCount must_== 1
        t0.metrics.sliceTraversedCount must_== expectedSlices
      }
    }

    "render to CSV in a simple case" in {
      val events = """
{"a": 1, "b": {"bc": 999, "bd": "foooooo", "be": true, "bf": null, "bg": false}, "c": [1.999], "d": "dog"}
{"a": 2, "b": {"bc": 998, "bd": "fooooo", "be": null, "bf": false, "bg": true}, "c": [2.999], "d": "dogg"}
{"a": 3, "b": {"bc": 997, "bd": "foooo", "be": false, "bf": true, "bg": null}, "c": [3.999], "d": "doggg"}
{"a": 4, "b": {"bc": 996, "bd": "fooo", "be": true, "bf": null, "bg": false}, "c": [4.999], "d": "dogggg"}
""".trim

      val expected = "" +
      ".a,.b.bc,.b.bd,.b.be,.b.bf,.b.bg,.c[0],.d\r\n" +
      "1,999,foooooo,true,null,false,1.999,dog\r\n" +
      "2,998,fooooo,null,false,true,2.999,dogg\r\n" +
      "3,997,foooo,false,true,null,3.999,doggg\r\n" +
      "4,996,fooo,true,null,false,4.999,dogggg\r\n"

      testRenderCsv(events) must_== expected
    }

    "test rendering uniform tables of varying sizes" in {
      renderLotsToCsv(100)
      renderLotsToCsv(1000)
      renderLotsToCsv(10000)
      renderLotsToCsv(100000)
    }

    "test string escaping" in {
      val csv = testRenderCsv("{\"s\":\"a\\\"b\",\"t\":\",\",\"u\":\"aa\\nbb\",\"v\":\"a,b\\\"c\\r\\nd\"}")

      val expected = "" +
".s,.t,.u,.v\r\n" +
"\"a\"\"b\",\",\",\"aa\n" +
"bb\",\"a,b\"\"c\r\n" +
"d\"\r\n"

      csv must_== expected
    }

    "test mixed rows" in {

      val input = """
{"a": 1}
{"b": 99.1}
{"a": true}
{"c": "jgeiwgjewigjewige"}
{"b": "foo", "d": 999}
{"e": null}
{"f": {"aaa": 9}}
{"c": 100, "g": 934}
""".trim

      val expected = "" +
".a,.b,.c,.d,.e,.f.aaa,.g\r\n" +
"1,,,,,,\r\n" +
",99.1,,,,,\r\n" +
"true,,,,,,\r\n" +
",,jgeiwgjewigjewige,,,,\r\n" +
",foo,,999,,,\r\n" +
",,,,null,,\r\n" +
",,,,,9,\r\n" +
",,100,,,,934\r\n"

      testRenderCsv(input) must_== expected

      val expected2 = "" +
".a,.b\r\n1,\r\n,99.1\r\n\r\n" +
".a,.c\r\ntrue,\r\n,jgeiwgjewigjewige\r\n\r\n" +
".b,.d,.e\r\nfoo,999,\r\n,,null\r\n\r\n" +
".c,.f.aaa,.g\r\n,9,\r\n100,,934\r\n"

      testRenderCsv(input, Some(2)) must_== expected2
    }
  }
}
