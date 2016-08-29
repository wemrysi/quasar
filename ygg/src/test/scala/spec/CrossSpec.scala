package ygg.tests

import scalaz._, Scalaz._
import ygg._, common._, json._, table._

class CrossSpec extends ColumnarTableQspec {
  import SampleData._
  import trans._

  private implicit def cogroupData: Arbitrary[CogroupData] = Arbitrary(genCogroupData)

  "in cross" >> {
    "perform a simple cartesian"                              in testSimpleCross
    "split a cross that would exceed maxSliceSize boundaries" in testCrossLarge
    "cross across slice boundaries on one side"               in testCrossSingles
    "survive scalacheck"                                      in prop((cd: CogroupData) => testCross(cd._1, cd._2))
  }

  private def testCross(l: SampleData, r: SampleData) = {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    def removeUndefined(jv: JValue): JValue = jv match {
      case JObject.Fields(jfields) => JObject(jfields collect { case JField(s, v) if v != JUndefined => JField(s, removeUndefined(v)) })
      case JArray(jvs) =>
      JArray(jvs map { jv =>
        removeUndefined(jv)
        })
      case v => v
    }

    val expected: Stream[JValue] = (
      for (lv <- l.data; rv <- r.data) yield
        JObject(JField("left", removeUndefined(lv)) :: JField("right", removeUndefined(rv)) :: Nil)
    )

    val result = ltable.cross(rtable)(
      InnerObjectConcat(WrapObject(Leaf(SourceLeft), "left"), WrapObject(Leaf(SourceRight), "right"))
    )

    toJson(result).copoint must_=== expected
  }

  private def testSimpleCross = {
    val s1 = SampleData(Stream(toRecord(Array(1), json"""{"a":[]}"""), toRecord(Array(2), json"""{"a":[]}""")))
    val s2 = SampleData(Stream(toRecord(Array(1), json"""{"b":0}"""), toRecord(Array(2), json"""{"b":1}""")))

    testCross(s1, s2)
  }

  private def testCrossLarge = {
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

  private def testCrossSingles = {
    val s1 = SampleData(
      Stream(
        toRecord(Array(1), json"""{ "a": 1 }"""),
        toRecord(Array(2), json"""{ "a": 2 }"""),
        toRecord(Array(3), json"""{ "a": 3 }"""),
        toRecord(Array(4), json"""{ "a": 4 }"""),
        toRecord(Array(5), json"""{ "a": 5 }"""),
        toRecord(Array(6), json"""{ "a": 6 }"""),
        toRecord(Array(7), json"""{ "a": 7 }"""),
        toRecord(Array(8), json"""{ "a": 8 }"""),
        toRecord(Array(9), json"""{ "a": 9 }"""),
        toRecord(Array(10), json"""{ "a": 10 }"""),
        toRecord(Array(11), json"""{ "a": 11 }""")
        ))

    val s2 = SampleData(Stream(toRecord(Array(1), json"""{"b":1}"""), toRecord(Array(2), json"""{"b":2}""")))

    testCross(s1, s2)
    testCross(s2, s1)
  }
}
