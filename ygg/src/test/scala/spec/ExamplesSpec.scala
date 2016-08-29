package ygg.tests

import ygg._, common._, json._
import Examples._

class ExamplesSpec extends quasar.Qspec {
  import JParser._

  "Lotto example"  in (lotto must_=== parseUnsafe(lotto.render))
  "Person example" in (person must_=== parseUnsafe(person.render))

  "Transformation example" in {
    val uppercased = person.transform(JField.liftCollect { case JField(n, v) => JField(n.toUpperCase, v) })
    val rendered   = uppercased.render
    rendered.contains(""""NAME":"Joe"""") must_=== true
    rendered.contains(""""AGE":35.0""") must_=== true
  }

  "Remove example" in {
    val json = person remove { _ == JString("Marilyn") }
    (json \\ "name").render must_=== "\"Joe\""
  }

  "Quoted example" in {
    val quoted: JValue   = json"""["foo \" \n \t \r bar"]"""
    val expected: JValue = jarray(JString("foo \" \n \t \r bar"))

    quoted must_=== expected
  }

  "Null example" in {
    json""" {"name": null} """.render must_=== """{"name":null}"""
  }

  "Symbol example" in {
    val symbols  = json"""{"f1":"foo","f2":"bar"}"""
    val expected = """{"f1":"foo","f2":"bar"}"""

    symbols.render must_=== expected
  }

  "Unicode example" in {
    parseUnsafe("[\" \\u00e4\\u00e4li\\u00f6t\"]") must_=== JArray(List(JString(" \u00e4\u00e4li\u00f6t")))
  }

  "Exponent example" in {
    json"""{"num": 2e5 }"""    must_=== jobject("num" -> JNum("2e5"))
    json"""{"num": -2E5 }"""   must_=== jobject("num" -> JNum("-2E5"))
    json"""{"num": 2.5e5 }"""  must_=== jobject("num" -> JNum("2.5e5"))
    json"""{"num": 2.5e-5 }""" must_=== jobject("num" -> JNum("2.5e-5"))
  }

  "JSON building example" in {
    val data     = json"""[{"name":"joe"},{"age":34},{"name":"mazy"},{"age":31}]"""
    val expected = """[{"name":"joe"},{"age":34},{"name":"mazy"},{"age":31}]"""

    data.render must_=== expected
  }

  "Example which collects all integers and forms a new JSON" in {
    val json = person
    val ints = json.foldDown(JUndefined: JValue) { (a, v) =>
      v match {
        case x: JNum => a ++ x
        case _       => a
      }
    }
    val out = ints.render
    out == "[33.0,35.0]" || out == "[35.0,33.0]" must_=== true
  }

  "Example which folds up to form a flattened list" in {
    val json = person

    def form(list: JPath*): Vector[JPathValue] = list.toVector map (p => p -> json(p))
    def folded = json.foldUpWithPath(Vector[JPathValue]())((res, path, json) => (path -> json) +: res).sorted

    val formed = form(
      NoJPath,
      "person",
      "person.age",
      "person.name",
      "person.spouse",
      "person.spouse.person",
      "person.spouse.person.age",
      "person.spouse.person.name"
    )

    folded must_=== formed
  }
}
