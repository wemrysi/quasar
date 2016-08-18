package ygg.tests

import blueeyes._, json._
import JsonTestSupport._

class JPathSpec extends quasar.Qspec {
  "Parser" should {
    "parse all valid JPath strings" in {
      prop((p: JPath) => JPath(p.toString) == p)
    }
    "forgivingly parse initial field name without leading dot" in {
      JPath("foo.bar").nodes must_=== List[JPathNode]("foo", "bar")
    }
  }

  "Extractor" should {
    "extract all existing paths" in {

      implicit val arb: Arbitrary[JValue -> List[JPath -> JValue]] = Arbitrary {
        for (jv <- arbitrary[JObject]) yield (jv, jv.flattenWithPath)
      }

      prop { (testData: (JValue, List[JPath -> JValue])) =>
        testData match {
          case (obj, allPathValues) =>
            val allProps = allPathValues.map {
              case (path, pathValue) => path.extract(obj) == pathValue
            }
            allProps.foldLeft[Prop](true)(_ && _)
        }
      }
    }

    "extract a second level node" in {
      val j = JObject(JField("address", JObject(JField("city", JString("B")) :: JField("street", JString("2")) :: Nil)) :: Nil)

      JPath("address.city").extract(j) must_=== (JString("B"))
    }
  }

  "Parent" should {
    "return parent" in {
      JPath(".foo.bar").parent must beSome(JPath(".foo"))
    }

    "return Identity for path 1 level deep" in {
      JPath(".foo").parent must beSome(NoJPath)
    }

    "return None when there is no parent" in {
      NoJPath.parent must_=== None
    }
  }

  "Ancestors" should {
    "return two ancestors" in {
      JPath(".foo.bar.baz").ancestors must_=== List(JPath(".foo.bar"), JPath(".foo"), NoJPath)
    }

    "return empty list for identity" in {
      NoJPath.ancestors must_=== Nil
    }
  }

  "dropPrefix" should {
    "return just the remainder" in {
      JPath(".foo.bar[1].baz").dropPrefix(".foo.bar") must beSome(JPath("[1].baz"))
    }

    "return none on path mismatch" in {
      JPath(".foo.bar[1].baz").dropPrefix(".foo.bar[2]") must beNone
    }
  }

  "Ordering" should {
    "sort according to nodes names/indexes" in {
      val test = List[JPath](
        "[1]",
        "[0]",
        "a",
        "a[9]",
        "a[10]",
        "b[10]",
        "a[10].a[1]",
        "b[10].a[1]",
        "b[10].a.x",
        "b[10].a[0]",
        "b[10].a[0].a"
      )
      val expected = List(1, 0, 2, 3, 4, 6, 5, 9, 10, 7, 8) map test

      test.sorted must_=== expected
    }
  }
}
