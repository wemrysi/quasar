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

package quasar.macros

import quasar._, Predef._
import quasar.ejson.EJson
import ygg.macros._
import JsonMacros.EJson._

sealed trait FieldTree
final case class FieldAtom() extends FieldTree
final case class FieldSeq(xs: Vector[FieldTree]) extends FieldTree
final case class FieldMap(xs: Vector[(FieldTree, FieldTree)]) extends FieldTree

class JsonMacroSpec extends quasar.Qspec {
  val xint: Int                          = 5
  val xlong: Long                        = 6L
  val xbigint: BigInt                    = BigInt(7)
  val xdouble: Double                    = 8.5d
  val xbigdec: BigDecimal                = BigDecimal("9.5")
  val xbool: Boolean                     = true
  val xarr: Array[Boolean]               = Array(true)
  val xobj: Map[String, Int]             = Map("z" -> 3)
  val xobj2: Map[String, Array[Boolean]] = Map("z" -> xarr)

  val jint    = json"""$xint"""
  val jlong   = json"""$xlong"""
  val jbigint = json"""$xbigint"""
  val jdouble = json"""$xdouble"""
  val jbigdec = json"""$xbigdec"""
  val jbool   = json"""$xbool"""
  val jarr    = json"""$xarr"""
  val jobj    = json"""$xobj"""
  val jobj2   = json"""$xobj2"""

  "json files" should {
    "load" >> {
      val js1 = {
        jawn.Parser.parseFromPath[EJson[Data]]("testdata/patients-mini.json").toOption
      }
      val js2 = {
        import JsonMacros.Argonaut._
        jawn.Parser.parseFromPath[argonaut.Json]("testdata/patients-mini.json").toOption
      }
      val js3 = {
        jawn.Parser.parseFromPath[Data]("testdata/patients-mini.json").toOption
      }

      js1 must beSome(beAnInstanceOf[EJson[Data]])
      js2 must beSome(beAnInstanceOf[argonaut.Json])
      js3 must beSome(beAnInstanceOf[Data])
    }
  }

  "json macros" should {
    "parse literals" >> {
      val x = json"""{ "a" : 1, "b" : 2, "c": null }"""
      ok
    }
    "interpolate" >> {
      json"""{ "bob": $xobj, "tom": $jobj }""" must_=== json"""${ Map("bob" -> jobj, "tom" -> jobj) }"""
    }
    "interpolate identity" >> {
      val x = json"[ 5 ]"
      json"$x" must_=== json"[ 5 ]"
    }
    "handle sequences" >> {
      jsonSeq"""1 2 3""" must_=== Vector(json"1", json"2", json"3")
      jsonSeq"""
        [ 1, 2 ]
        [ 3, 4 ]
      """ must_=== Vector(json"[1, 2]", json"[3, 4]")

      jsonSeq"""$xbool $xarr $xobj $xobj2""" must_=== Vector(jbool, jarr, jobj, jobj2)
    }
  }


/***
  type EJ = quasar.ejson.EJson[Data]

  def childNames(x: Data): Set[String] = x match {
    case Data.Arr(xs) => xs.map(childNames) reduceLeft (_ intersect _)
    case Data.Obj(xs) => xs.keys.toSet
    case _            => Set()
  }
  def childNamesEJ(x: EJ): Vector[String] = {
    def extension(x: ejson.Extension[Data]): Vector[String] = x match {
      case ejson.Map(xs) => childNames(Data.Obj(scala.collection.immutable.ListMap(xs collect { case (Data.Str(k), v) => k -> v } : _*))).toVector.sorted
      case _             => Vector()
    }
    def common(x: ejson.Common[Data]): Vector[String] = x match {
      case ejson.Arr(xs) => childNames(Data.Arr(xs)).toVector.sorted
      case _             => Vector()
    }
    x.run.fold(extension, common)

  }

  def fieldTree(x: Data): FieldTree = x match {
    case Data.Arr(xs) => FieldSeq(xs.toVector map fieldTree)
    case Data.Obj(xs) => FieldMap(xs.toVector map { case (k, v) => fieldTree(Data.Str(k)) -> fieldTree(v) })
    case _            => FieldAtom()
  }
  def fieldTreeEJ(x: EJ): FieldTree = {
    def extension(x: ejson.Extension[Data]): FieldTree = x match {
      case ejson.Map(xs) => FieldMap(xs.toVector map { case (k, v) => fieldTree(k) -> fieldTree(v) })
      case _             => FieldAtom()
    }
    def common(x: ejson.Common[Data]): FieldTree = x match {
      case ejson.Arr(xs) => FieldSeq(xs.toVector map fieldTree)
      case _             => FieldAtom()
    }

    x.run.fold(extension, common)
  }

  "json files" should {
    "load" >> {
      val js = jawn.Parser.parseFromPath[EJ]("testdata/patients-mini.json")
      val fs = childNamesEJ(js.get)
      println(fs)
      ok
    }
  }

***/
}
