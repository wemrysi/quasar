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
import ygg._, common._

class TakeRangeSpec extends TableQspec {
  import SampleData._

  "in takeRange" >> {
    // "takeRange commutes as expected"                                                      in testTakeRangeCommutes
    "select the correct rows: trivial case"                                               in testTakeRange
    "select the correct rows when we take past the end of the table"                      in testTakeRangeLarger
    "select the correct rows when we start at an index larger than the size of the table" in testTakeRangeEmpty
    "select the correct rows across slice boundary"                                       in testTakeRangeAcrossSlices
    "select the correct rows: second slice"                                               in testTakeRangeSecondSlice
    "select the first slice"                                                              in testTakeRangeFirstSliceOnly
    "select nothing with a negative starting index"                                       in testTakeRangeNegStart
    "select nothing with a negative number to take"                                       in testTakeRangeNegTake
    "select the correct rows using scalacheck"                                            in checkTakeRange
  }

  private def jsonFourValues = jsonMany"""
    {"key":[1],"value":"foo"}
    {"key":[2],"value":12}
    {"key":[3],"value":{"baz":true}}
    {"key":[4],"value":"ack"}
  """
  private def jsonEightValues = (jsonFourValues ++ jsonMany"""
    {"key":[5],"value":"ack2"}
    {"key":[6],"value":"ack3"}
    {"key":[7],"value":"ack4"}
    {"key":[8],"value":"ack5"}
  """).toStream

  private def checkTakeRange = {
    implicit val gen: Arbitrary[SampleData] = sample(schema)

    prop { (sample: SampleData) =>

      val table          = fromSample(sample)
      val size           = sample.data.size
      val start          = choose(-7, size + 7).sample.get
      val count          = choose(start, size + 7).sample.get
      val takeRangeTable = table.takeRange(start.toLong, count.toLong)
      val result         = toJson(takeRangeTable).copoint

      val expected =
        if (start < 0) Stream()
        else sample.data.toSeq.drop(start).take(count)

      result must_== expected
    }
  }

  /***
  private def testTakeRangeCommutes: Prop = {
    implicit val arbRange = Arbitrary(genOffsetAndLen)

    prop { (offlen: Int -> Int) =>
      val (start, length) = offlen
      def doSlice(xs: Seq[JValue]): Seq[JValue] =
        if (start < 0 || length <= 0) Seq() else xs drop start take length

      checkCommutes(
        doSlice,
        _.takeRange(start.toLong, length.toLong),
        genJValueSeq
      )
    }
  }
  ***/

  private def testTakeRange = checkTableFun(
    fun      = _.takeRange(1, 2),
    data     = jsonFourValues,
    expected = jsonFourValues.slice(1, 3)
  )
  private def testTakeRangeNegStart = checkTableFun(
    fun      = _.takeRange(-1, 5),
    data     = jsonFourValues,
    expected = Seq()
  )
  private def testTakeRangeNegTake = checkTableFun(
    fun      = _.takeRange(2, -3),
    data     = jsonFourValues,
    expected = Seq()
  )
  private def testTakeRangeLarger = checkTableFun(
    fun      = _.takeRange(2, 17),
    data     = jsonFourValues,
    expected = jsonFourValues.slice(2, 4)
  )
  private def testTakeRangeEmpty = checkTableFun(
    fun      = _.takeRange(6, 17),
    data     = jsonFourValues,
    expected = Seq()
  )
  private def testTakeRangeAcrossSlices = checkTableFun(
    fun      = _.takeRange(1, 6),
    table    = fromSample(SampleData(jsonEightValues), Some(5)),
    expected = jsonEightValues.slice(1, 7)
  )
  private def testTakeRangeSecondSlice = checkTableFun(
    fun      = _.takeRange(5, 2),
    table    = fromSample(SampleData(jsonEightValues), Some(5)),
    expected = jsonEightValues.slice(5, 7)
  )
  private def testTakeRangeFirstSliceOnly = checkTableFun(
    fun      = _.takeRange(0, 5),
    table    = fromSample(SampleData(jsonEightValues), Some(5)),
    expected = jsonEightValues.slice(0, 5)
  )
}
