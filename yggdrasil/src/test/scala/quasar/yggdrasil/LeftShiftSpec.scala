/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.yggdrasil

import quasar.precog.common._
import quasar.blueeyes.json._

import org.specs2._

import _root_.scalaz._, Scalaz._

trait LeftShiftSpec extends TableModuleTestSupport with SpecificationLike {

  def testTrivialArrayLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: Nil))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))))

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialEmptyArrayLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JArray(Nil))
    val table = fromSample(SampleData(Stream(rec)))

    val expected = Vector()

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialStringLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JString("s"))
    val table = fromSample(SampleData(Stream(rec)))

    val expected = Vector()

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialUndefinedLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JUndefined)
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      if (emit) Vector(toRecord0(Array(0)))
      else Vector()

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialObjectLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JObject(JField("foo", JNum(12)), JField("bar", JNum(13))))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JString("bar"), JNum(13))),
        toRecord(Array(0), JArray(JString("foo"), JNum(12))))

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialEmptyObjectLeftShift(emit: Boolean) = {
    val rec = toRecord(Array(0), JObject())
    val table = fromSample(SampleData(Stream(rec)))

    val expected = Vector()

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialObjectArrayLeftShift(emit: Boolean) = {
    val table =
      fromSample(
        SampleData(
          Stream(
            toRecord(Array(0), JObject(JField("foo", JNum(12)), JField("bar", JNum(13)))),
            toRecord(Array(1), JArray(JNum(42), JNum(43))))))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JString("bar"), JNum(13))),
        toRecord(Array(0), JArray(JString("foo"), JNum(12))),
        toRecord(Array(1), JArray(JNum(0), JNum(42))),
        toRecord(Array(1), JArray(JNum(1), JNum(43))))

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testHeterogenousLeftShift(emit: Boolean) = {
    val table =
      fromSample(
        SampleData(
          Stream(
            toRecord(Array(0), JString("s")),
            toRecord(Array(1), JObject(JField("foo", JNum(12)), JField("bar", JNum(13)))),
            toRecord(Array(2), JArray()),
            toRecord(Array(3), JArray(JNum(42), JNum(43))),
            toRecord(Array(4), JObject()),
            toRecord(Array(5), JUndefined))))

    val expected =
      Vector(
        toRecord(Array(1), JArray(JString("bar"), JNum(13))),
        toRecord(Array(1), JArray(JString("foo"), JNum(12))),
        toRecord(Array(3), JArray(JNum(0), JNum(42))),
        toRecord(Array(3), JArray(JNum(1), JNum(43)))) ++
      (if (emit) Vector(toRecord0(Array(5))) else Vector())

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testSetArrayLeftShift(emit: Boolean) = {
    def rec(i: Int) =
      toRecord(Array(i), JArray(JNum(i * 12) :: JNum(i * 13) :: Nil))

    val table = fromSample(SampleData(Stream.from(0).map(rec).take(100)))

    def expected(i: Int) =
      Vector(
        toRecord(Array(i), JArray(JNum(0), JNum(i * 12))),
        toRecord(Array(i), JArray(JNum(1), JNum(i * 13))))

    val expectedAll = (0 until 100).toVector.flatMap(expected)

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expectedAll
  }

  def testHeteroArrayLeftShift(emit: Boolean) = {
    val table =
      fromSample(
        SampleData(
          Stream(
            toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: Nil)),
            toRecord(Array(1), JArray(JNum(22) :: JNum(23) :: JNum(24) :: JNum(25) :: Nil)),
            toRecord(Array(2), JArray(Nil)),
            toRecord(Array(3), JString("psych!")),
            toRecord(Array(4), JUndefined))))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))),
        toRecord(Array(1), JArray(JNum(0), JNum(22))),
        toRecord(Array(1), JArray(JNum(1), JNum(23))),
        toRecord(Array(1), JArray(JNum(2), JNum(24))),
        toRecord(Array(1), JArray(JNum(3), JNum(25)))) ++ (
      if (emit) Vector(toRecord0(Array(4)))
      else Vector())

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  def testTrivialArrayLeftShiftWithInnerObject(emit: Boolean) = {
    val rec = toRecord(Array(0), JArray(JNum(12) :: JNum(13) :: JObject(JField("a", JNum(42))) :: Nil))
    val table = fromSample(SampleData(Stream(rec)))

    val expected =
      Vector(
        toRecord(Array(0), JArray(JNum(0), JNum(12))),
        toRecord(Array(0), JArray(JNum(1), JNum(13))),
        toRecord(Array(0), JArray(JNum(2), JObject(JField("a", JNum(42))))))

    toJson(table.leftShift(CPath.Identity \ 1, emitOnUndef = emit)).getJValues mustEqual expected
  }

  // replaces SampleData.toRecord to avoid ordering issues
  def toRecord(indices: Array[Int], jv: JValue): JValue =
    JArray(JArray(indices.map(JNum(_)).toList) :: jv :: Nil)

  def toRecord0(indices: Array[Int]): JValue =
    JArray(JArray(indices.map(JNum(_)).toList) :: Nil)
}
