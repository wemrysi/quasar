package ygg.tests

import ygg.table._
import scalaz._, Scalaz._
import SampleData._
import ygg.json._

class ColumnarTableModuleSpec
      extends ColumnarTableQspec
         with TableModuleSpec
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

  "a table dataset" should {
    "verify bijection from JSON" in checkMappings(this)
    "verify bijection from static JSON" in {
      implicit val gen = sample(schema)
      prop((sd: SampleData) => toJsonSeq(fromJson(sd.data)) must_=== sd.data)
    }
    "verify renderJson round tripping" in {
      implicit val gen = sample(schema)
      prop((sd: SampleData) => testRenderJson(sd.data: _*))
    }

    "handle special cases of renderJson" >> {
      "undefined at beginning of array"  >> testRenderJson(jarray(undef, JNum(1), JNum(2)))
      "undefined in middle of array"     >> testRenderJson(jarray(JNum(1), undef, JNum(2)))
      "fully undefined array"            >> testRenderJson(jarray(undef, undef, undef))
      "undefined at beginning of object" >> testRenderJson(jobject("foo" -> undef, "bar" -> JNum(1), "baz" -> JNum(2)))
      "undefined in middle of object"    >> testRenderJson(jobject("foo" -> JNum(1), "bar" -> undef, "baz" -> JNum(2)))
      "fully undefined object"           >> testRenderJson(jobject())
      "undefined row"                    >> testRenderJson(jobject(), JNum(42))

      "check utf-8 encoding" in prop((s: String) => testRenderJson(json"${ sanitize(s) }"))
      "check long encoding"  in prop((x: Long) => testRenderJson(json"$x"))
    }

    "in cross" >> {
      "perform a simple cartesian"                              in testSimpleCross
      "split a cross that would exceed maxSliceSize boundaries" in testCrossLarge
      "cross across slice boundaries on one side"               in testCrossSingles
      "survive scalacheck"                                      in prop((cd: CogroupData) => testCross(cd._1, cd._2))
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

      "delete elements according to a JType"                     in checkObjectDelete
      "delete only field of object without removing from array"  in checkObjectDeleteWithoutRemovingArray
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
  }
}
