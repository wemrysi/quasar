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

import scalaz._, Scalaz._
import ygg._, common._, json._
import ygg.table.Render

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
    val arrayM    = Render.renderTable(table, "[", ",", "]").foldLeft("")(_ + _.toString).map(JParser.parseUnsafe)
    val minimized = minimize(expected) getOrElse jarray()

    arrayM.copoint mustEqual minimized
  }
}
