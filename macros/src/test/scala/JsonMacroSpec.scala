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

package ygg.macros

import scala.StringContext
import scala.reflect.ClassTag
import scala.Predef.implicitly

import slamdata.Predef._
import quasar._

class ArgonautJsonSpec extends AbstractJsonSpec[argonaut.Json]()(argonaut.JawnParser.facade, implicitly[ClassTag[argonaut.Json]])
class DataJsonSpec extends AbstractJsonSpec[Data]()
class EJsonDataJsonSpec extends AbstractJsonSpec[quasar.ejson.EJson[Data]]()(EJsonDataFacade, implicitly)
class JawnJsonSpec extends AbstractJsonSpec[jawn.ast.JValue]()
// class PrecogJsonSpec extends AbstractJsonSpec[ygg.json.JValue]()

abstract class AbstractJsonSpec[A](implicit facade: jawn.Facade[A], ctag: ClassTag[A]) extends FacadeBasedInterpolator[A] with quasar.QuasarSpecification {
  val xint: Int                          = 5
  val xlong: Long                        = 6L
  val xbigint: BigInt                    = BigInt(7)
  val xdouble: Double                    = 8.5d
  val xbigdec: BigDecimal                = BigDecimal("9.5")
  val xbool: Boolean                     = true
  val xarr: Array[Boolean]               = Array(true)
  val xobj: Map[String, Int]             = Map("z" -> 3)
  val xobj2: Map[String, Array[Boolean]] = Map("z" -> xarr)

  val jint    = json"$xint"
  val jlong   = json"$xlong"
  val jbigint = json"$xbigint"
  val jdouble = json"$xdouble"
  val jbigdec = json"$xbigdec"
  val jbool   = json"$xbool"
  val jarr    = json"$xarr"
  val jobj    = json"$xobj"
  val jobj2   = json"$xobj2"

  "json files" should {
    "return Some(json) on json file" >> {
      val js = jawn.Parser.parseFromPath[A]("macros/src/test/resources/patients-mini.json").toOption
      js must beSome.which(ctag.runtimeClass isAssignableFrom _.getClass)
      // Doesn't work this way if A is abstract.
      // js must beSome(beAnInstanceOf[A])
    }
    "return None on missing file" >> {
      val js = jawn.Parser.parseFromPath[A]("macros/src/test/resources/does-not-exist.json").toOption
      js must beNone
    }
  }

  "json macros" should {
    "parse literals" >> {
      val x = json"""{ "a" : 1, "b" : 2, "c": null }"""
      ok
    }
    "interpolate" >> {
      ( json"""{ "bob": $xobj, "tom": $jobj }""" must_===
        json"""${ Map("bob" -> jobj, "tom" -> jobj) }"""
      )
    }
    "interpolate keys" >> {
      val k1 = "bob"
      val k2 = "tom"
      val v1 = Array(false, false)
      val v2 = Array(Array(1), Array(2))
      val xs = json"""{ $k1: $v1, $k2: $v2 }"""
      val xs2 = json"[ $xs, $xs ]"

      val expected = json"""{ "bob": [ false, false ], "tom": [ [ 1 ], [ 2 ] ] }"""

      xs must_=== expected
      xs2 must_=== json"[ $expected, $expected ]"
    }
    "interpolate identity" >> {
      val x = json"[ 5 ]"
      json"$x" must_=== json"[ 5 ]"
    }
    "handle sequences" >> {
      jsonMany"""1 2 3""" must_=== Vector(json"1", json"2", json"3")
      jsonMany"""
        [ 1, 2 ]
        [ 3, 4 ]
      """ must_=== Vector(json"[1, 2]", json"[3, 4]")

      jsonMany"""$xbool $xarr $xobj $xobj2""" must_=== Vector(jbool, jarr, jobj, jobj2)
    }
  }
}
