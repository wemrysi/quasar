package ygg.tests

import scalaz._, Scalaz._
import ygg.json._

trait CrossSpec extends TableQspec {
  import SampleData._
  import trans._

  def testCross(l: SampleData, r: SampleData) = {
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

    val expected: Stream[JValue] = for {
      lv <- l.data
      rv <- r.data
    } yield {
      JObject(JField("left", removeUndefined(lv)) :: JField("right", removeUndefined(rv)) :: Nil)
    }

    val result = ltable.cross(rtable)(
      InnerObjectConcat(WrapObject(Leaf(SourceLeft), "left"), WrapObject(Leaf(SourceRight), "right"))
    )

    val jsonResult: Need[Stream[JValue]] = toJson(result)
    jsonResult.copoint must_== expected
  }

  def testSimpleCross = {
    val s1 = SampleData(Stream(toRecord(Array(1), json"""{"a":[]}"""), toRecord(Array(2), json"""{"a":[]}""")))
    val s2 = SampleData(Stream(toRecord(Array(1), json"""{"b":0}"""), toRecord(Array(2), json"""{"b":1}""")))

    testCross(s1, s2)
  }

  def testCrossSingles = {
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
