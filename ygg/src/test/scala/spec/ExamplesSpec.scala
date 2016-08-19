package ygg.tests

import blueeyes._, json.{ JPath => _, _ }
import Examples._
import ygg.json._

class ExamplesSpec extends quasar.Qspec {
  import JParser._

  "Lotto example" in {
    val json          = parseUnsafe(lotto)
    val renderedLotto = json.renderCompact
    json mustEqual parseUnsafe(renderedLotto)
  }

  "Person example" in {
    val json           = parseUnsafe(person)
    val renderedPerson = json.renderPretty
    json mustEqual parseUnsafe(renderedPerson)
    //render(json) mustEqual render(personDSL)
    //compact(render(json \\ "name")) mustEqual """["Joe","Marilyn"]"""
    //compact(render(json \ "person" \ "name")) mustEqual "\"Joe\""
  }

  "Transformation example" in {
    val uppercased = parseUnsafe(person).transform(JField.liftCollect { case JField(n, v) => JField(n.toUpperCase, v) })
    val rendered   = uppercased.renderCompact
    rendered.contains(""""NAME":"Joe"""") mustEqual true
    rendered.contains(""""AGE":35.0""") mustEqual true
    //rendered mustEqual
    //  """{"PERSON":{"NAME":"Joe","AGE":35.0,"SPOUSE":{"PERSON":{"NAME":"Marilyn","AGE":33.0}}}}"""
  }

  "Remove example" in {
    val json = parseUnsafe(person) remove { _ == JString("Marilyn") }
    (json \\ "name").renderCompact mustEqual "\"Joe\""
  }

  "Quoted example" in {
    parseUnsafe(quoted) mustEqual JArray(List(JString("foo \" \n \t \r bar")))
  }

  "Null example" in {
    parseUnsafe(""" {"name": null} """).renderCompact mustEqual """{"name":null}"""
  }

  "Symbol example" in {
    symbols.renderCompact mustEqual """{"f1":"foo","f2":"bar"}"""
  }

  "Unicode example" in {
    parseUnsafe("[\" \\u00e4\\u00e4li\\u00f6t\"]") mustEqual JArray(List(JString(" \u00e4\u00e4li\u00f6t")))
  }

  "Exponent example" in {
    parseUnsafe("""{"num": 2e5 }""") mustEqual JObject(List(JField("num", JNum("2e5"))))
    parseUnsafe("""{"num": -2E5 }""") mustEqual JObject(List(JField("num", JNum("-2E5"))))
    parseUnsafe("""{"num": 2.5e5 }""") mustEqual JObject(List(JField("num", JNum("2.5e5"))))
    parseUnsafe("""{"num": 2.5e-5 }""") mustEqual JObject(List(JField("num", JNum("2.5e-5"))))
  }

  "JSON building example" in {
    val json = JObject(JField("name", JString("joe")) :: Nil) ++ JObject(JField("age", JNum(34)) :: Nil) ++
        JObject(JField("name", JString("mazy")) :: Nil) ++ JObject(JField("age", JNum(31)) :: Nil)

    json.renderCompact mustEqual """[{"name":"joe"},{"age":34},{"name":"mazy"},{"age":31}]"""
  }

  "Example which collects all integers and forms a new JSON" in {
    val json = parseUnsafe(person)
    val ints = json.foldDown(JUndefined: JValue) { (a, v) =>
      v match {
        case x: JNum => a ++ x
        case _       => a
      }
    }
    val out = ints.renderCompact
    out == "[33.0,35.0]" || out == "[35.0,33.0]" mustEqual true
  }

  "Example which folds up to form a flattened list" in {
    val json = parseUnsafe(person)

    def form(list: JPath*): List[JPath -> JValue] = list.toList.map { path =>
      (path, json(path))
    }

    val folded = (json
      .foldUpWithPath[List[JPath -> JValue]](Nil) { (list, path, json) =>
        (path, json) :: list
      })
      .sorted
      .collect { case (p, j) => (p, j) }

    val formed = form(
      NoJPath,
      JPath("person"),
      JPath("person.age"),
      JPath("person.name"),
      JPath("person.spouse"),
      JPath("person.spouse.person"),
      JPath("person.spouse.person.age"),
      JPath("person.spouse.person.name")
    )

    folded mustEqual formed
  }

  //"Renders JSON as Scala code" in {
  //  val json = parseUnsafe(lotto)
  //
  //  val output = Printer.compact(renderScala(json))
  //  output.contains("\"winning-numbers\",JArray(JNumStr(2)::JNumStr(45)::JNumStr(34)::JNumStr(23)::JNumStr(7)::JNumStr(5)::JNumStr(3)::Nil)") mustEqual true
  //  //output mustEqual """JObject("lotto",JObject("lotto-id",JNum(5)::"winning-numbers",JArray(JNum(2)::JNum(45)::JNum(34)::JNum(23)::JNum(7)::JNum(5)::JNum(3)::Nil)::"winners",JArray(JObject("winner-id",JNum(23)::"numbers",JArray(JNum(2)::JNum(45)::JNum(34)::JNum(23)::JNum(3)::JNum(5)::Nil)::Nil)::JObject("winner-id",JNum(54)::"numbers",JArray(JNum(52)::JNum(3)::JNum(12)::JNum(11)::JNum(18)::JNum(22)::Nil)::Nil)::Nil)::Nil)::Nil)"""
  //}
}
