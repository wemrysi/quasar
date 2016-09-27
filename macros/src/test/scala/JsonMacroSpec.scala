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
import ygg.macros._
import JsonMacros._
import EJson._

sealed trait FieldTree
final case class FieldAtom() extends FieldTree
final case class FieldSeq(xs: Vector[FieldTree]) extends FieldTree
final case class FieldMap(xs: Vector[FieldTree -> FieldTree]) extends FieldTree

class JsonMacroSpec extends quasar.Qspec {
  val xint: Int           = 5
  val xlong: Long         = 6L
  val xbigint: BigInt     = BigInt(7)
  val xdouble: Double     = 8.5d
  val xbigdec: BigDecimal = BigDecimal("9.5")
  val xbool: Boolean      = true

  val jint    = json"""$xint"""
  val jlong   = json"""$xlong"""
  val jbigint = json"""$xbigint"""
  val jdouble = json"""$xdouble"""
  val jbigdec = json"""$xbigdec"""
  val jbool   = json"""$xbool"""

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

  "json macros" should {
    "parse sequences" >> {
      jsonSeq"""1 2 3""" must_=== Vector(json"1", json"2", json"3")
    }
    "parse literals" >> {
      val x = json"""{ "a" : 1, "b" : 2, "c": null }"""
      ok
    }
    "interpolate" >> {
      val c = json"""[ 1, 2 ]"""
      val x = json"""{ "a" : 1, "b" : $c }"""

      x must_=== json"""{ "a" : 1, "b" : [ 1, 2 ] }"""
    }
    "interpolate more" >> {
      val c1 = 55
      val c2 = "bob"
      val c3 = Array[Int](1, 2)
      val c4 = Map[String, Int]("a" -> 1, "b" -> 2)
      val x = json"""[ $c1, $c2, $c3, $c4 ]"""

      x must_=== json"""[ 55, "bob", [ 1, 2 ], { "a": 1, "b": 2 } ]"""
    }
    "interpolate yet more" >> {
      val c1 = Map[String, Boolean]("a" -> false, "b" -> true)
      val c2 = json"""{ "x1": $c1, "y1": $c1 }"""
      val c3 = json"""{ "x2": $c2, "y2": $c2 }"""
      val c4 = json"""[ $c1, $c2, $c3 ]"""

      println(c4)
      ok
    }
  }
}
