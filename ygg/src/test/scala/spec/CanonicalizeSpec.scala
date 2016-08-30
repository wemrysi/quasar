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

import ygg._, common._, table._
import scalaz._, Scalaz._

class CanonicalizeSpec extends ColumnarTableQspec {
  import SampleData._

  "in canonicalize" >> {
    "return the correct slice sizes using scalacheck"                      in checkCanonicalize
    "return the slice size with correct bound using scalacheck with range" in checkBoundedCanonicalize
    "return the correct slice sizes: trivial case"                         in testCanonicalize
    "return the correct slice sizes given length zero"                     in testCanonicalizeZero
    "return the correct slice sizes along slice boundaries"                in testCanonicalizeBoundary
    "return the correct slice sizes greater than slice boundaries"         in testCanonicalizeOverBoundary
    "return empty table when given empty table"                            in testCanonicalizeEmpty
    "remove slices of size zero"                                           in testCanonicalizeEmptySlices
  }

  lazy val table: Table = fromJson(jsonMany"""
    {"foo":1}
    {"foo":2}
    {"foo":3}
    {"foo":4}
    {"foo":5}
    {"foo":6}
    {"foo":7}
    {"foo":8}
    {"foo":9}
    {"foo":10}
    {"foo":11}
    {"foo":12}
    {"foo":13}
    {"foo":14}
  """)

  def checkBoundedCanonicalize = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table     = fromSample(sample)
      val size      = sample.data.size
      val minLength = Gen.choose(0, size / 2).sample.get
      val maxLength = minLength + Gen.choose(1, size / 2 + 1).sample.get

      val canonicalizedTable = table.canonicalize(minLength, maxLength)
      val slices             = canonicalizedTable.slices.toStream.copoint map (_.size)
      if (size > 0) {
        slices.init must contain(like[Int]({ case size: Int => size must beBetween(minLength, maxLength) })).forall
        slices.last must be_<=(maxLength)
      } else {
        slices must haveSize(0)
      }
    }
  }

  def checkCanonicalize = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table  = fromSample(sample)
      val size   = sample.data.size
      val length = Gen.choose(1, size + 3).sample.get

      val canonicalizedTable = table.canonicalize(length)
      val resultSlices       = canonicalizedTable.slices.toStream.copoint
      val resultSizes        = resultSlices.map(_.size)

      val expected = {
        val num       = size / length
        val remainder = size % length
        val prefix    = Stream.fill(num)(length)
        if (remainder > 0) prefix :+ remainder else prefix
      }

      resultSizes mustEqual expected
    }
  }.set(minTestsOk = 200)

  def testCanonicalize = {
    val result = table.canonicalize(3)

    val slices = result.slices.toStream.copoint
    val sizes  = slices.map(_.size)

    sizes mustEqual Stream(3, 3, 3, 3, 2)
  }

  def testCanonicalizeZero = {
    table.canonicalize(0) must throwA[java.lang.AssertionError]
  }

  def testCanonicalizeBoundary = {
    val result = table.canonicalize(5)
    val slices = result.slices.toStream.copoint
    val sizes  = slices.map(_.size)

    sizes mustEqual Stream(5, 5, 4)
  }

  def testCanonicalizeOverBoundary = {
    val result = table.canonicalize(12)

    val slices = result.slices.toStream.copoint
    val sizes  = slices.map(_.size)

    sizes mustEqual Stream(12, 2)
  }

  def testCanonicalizeEmptySlices = {
    def tableTakeRange(table: Table, start: Long, numToTake: Long) =
      table.takeRange(start, numToTake).slices.toStream.copoint

    val slices =
      Stream(Slice.empty) ++ tableTakeRange(table, 0, 5) ++
        Stream(Slice.empty) ++ tableTakeRange(table, 5, 4) ++
        Stream(Slice.empty) ++ tableTakeRange(table, 9, 5) ++
        Stream(Slice.empty)

    val newTable     = Table(StreamT.fromStream(Need(slices)), table.size)
    val result       = newTable.canonicalize(4)
    val resultSlices = result.slices.toStream.copoint
    val resultSizes  = resultSlices.map(_.size)

    resultSizes mustEqual Stream(4, 4, 4, 2)
  }

  def testCanonicalizeEmpty = {
    val table  = Table.empty
    val result = table.canonicalize(3)
    val slices = result.slices.toStream.copoint
    val sizes  = slices.map(_.size)

    sizes mustEqual Stream()
  }
}
