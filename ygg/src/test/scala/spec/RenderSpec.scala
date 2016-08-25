package ygg.tests

import scalaz._, Scalaz._
import ygg.json._

class RenderSpec extends ColumnarTableQspec {
  import SampleData._

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

  private def testRenderJson(xs: JValue*) = {
    def minimizeItem(t: (String, JValue)) = minimize(t._2).map((t._1, _))
    def minimize(value: JValue): Option[JValue] = value match {
      case JUndefined       => None
      case JObject(fields)  => Some(JObject(fields.flatMap(minimizeItem)))
      case JArray(Seq())    => Some(jarray())
      case JArray(elements) => elements flatMap minimize match { case Seq() => None ; case xs => Some(JArray(xs)) }
      case v                => Some(v)
    }

    val table     = fromJson(xs.toVector)
    val expected  = JArray(xs.toVector)
    val arrayM    = table.renderJson("[", ",", "]").foldLeft("")(_ + _.toString).map(JParser.parseUnsafe)
    val minimized = minimize(expected) getOrElse jarray()

    arrayM.copoint mustEqual minimized
  }
}
