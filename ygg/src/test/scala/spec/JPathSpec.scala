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

import JsonTestSupport._
import ygg._, common._, json._

class JPathSpec extends quasar.Qspec {
  "Parser" should {
    "parse all valid JPath strings" in {
      prop((p: JPath) => JPath(p.toString) == p)
    }
    "forgivingly parse initial field name without leading dot" in {
      Seq[JPathNode]("foo", "bar") must_=== JPath("foo.bar").nodes
    }
  }

  "Extractor" should {
    "extract all existing paths" in {

      implicit val arb: Arbitrary[JValue -> Vector[JPathValue]] = Arbitrary {
        for (jv <- arbitrary[JObject]) yield (jv, jv.flattenWithPath)
      }

      prop { (testData: (JValue, Vector[JPathValue])) =>
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
      Seq(JPath(".foo.bar"), JPath(".foo"), NoJPath) must_=== JPath(".foo.bar.baz").ancestors
    }

    "return empty list for identity" in {
      Seq[JPath]() must_=== NoJPath.ancestors
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
      val test = Vec[JPath](
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
      val expected = Vec(1, 0, 2, 3, 4, 6, 5, 9, 10, 7, 8) map test

      test.sorted must_=== expected
    }
  }
}
