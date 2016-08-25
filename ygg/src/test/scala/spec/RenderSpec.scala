package ygg.tests

import ygg.json._

class RenderSpec extends ColumnarTableQspec {
  import SampleData._

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
}
