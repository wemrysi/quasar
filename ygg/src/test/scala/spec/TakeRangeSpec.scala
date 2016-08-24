package ygg.tests

import scalaz._, Scalaz._
import TestSupport._
import ygg.json._

trait TakeRangeSpec extends ColumnarTableQspec {
  import SampleData._

  private def someJson = jsonMany"""
    {"key":[1],"value":"foo"}
    {"key":[2],"value":12}
    {"key":[3],"value":{"baz":true}}
    {"key":[4],"value":"ack"}
  """.toStream

  def checkTakeRange = {
    implicit val gen: Arbitrary[SampleData] = sample(schema)

    prop { (sample: SampleData) =>
      val table = fromSample(sample)
      val size  = sample.data.size

      val start = Gen.choose(-7, size + 7).sample.get
      val count = Gen.choose(start, size + 7).sample.get

      val takeRangeTable = table.takeRange(start.toLong, count.toLong)

      val result = toJson(takeRangeTable).copoint
      val expected =
        if (start < 0) Stream()
        else sample.data.toSeq.drop(start).take(count)

      result must_== expected
    }
  }.set(minTestsOk = 1000)

  def testTakeRange = {
    val data    = someJson
    val table   = fromSample(SampleData(data))
    val results = toJson(table.takeRange(1, 2))

    val expected = jsonMany"""
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
    """.toStream

    results.copoint must_== expected
  }

  def testTakeRangeNegStart = {
    val data    = someJson
    val sample  = SampleData(data)
    val table   = fromSample(sample)
    val results = toJson(table.takeRange(-1, 5))

    results.copoint must_== Stream()
  }

  def testTakeRangeNegNumber = {
    val data    = someJson
    val sample  = SampleData(data)
    val table   = fromSample(sample)
    val results = toJson(table.takeRange(2, -3))

    results.copoint must_== Stream()
  }

  def testTakeRangeNeg = {
    val data    = someJson
    val sample  = SampleData(data)
    val table   = fromSample(sample)
    val results = toJson(table.takeRange(-1, 5))

    results.copoint must_== Stream()
  }

  def testTakeRangeLarger = {
    val data    = someJson
    val sample  = SampleData(data)
    val table   = fromSample(sample)
    val results = toJson(table.takeRange(2, 17))

    val expected = jsonMany"""
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack"}
    """.toStream

    results.copoint must_== expected
  }

  def testTakeRangeEmpty = {
    val data    = someJson
    val sample  = SampleData(data)
    val table   = fromSample(sample)
    val results = toJson(table.takeRange(6, 17))

    results.copoint must_== Stream()
  }

  def testTakeRangeAcrossSlices = {
    val data = jsonMany"""
      {"key":[1],"value":"foo"}
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack1"}
      {"key":[5],"value":"ack2"}
      {"key":[6],"value":"ack3"}
      {"key":[7],"value":"ack4"}
      {"key":[8],"value":"ack5"}
    """.toStream

    val sample  = SampleData(data)
    val table   = fromSample(sample, Some(5))
    val results = toJson(table.takeRange(1, 6))

    val expected = jsonMany"""
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack1"}
      {"key":[5],"value":"ack2"}
      {"key":[6],"value":"ack3"}
      {"key":[7],"value":"ack4"}
    """.toStream

    results.copoint must_== expected
  }

  def testTakeRangeSecondSlice = {
    val data = jsonMany"""
      {"key":[1],"value":"foo"}
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack1"}
      {"key":[5],"value":"ack2"}
      {"key":[6],"value":"ack3"}
      {"key":[7],"value":"ack4"}
      {"key":[8],"value":"ack5"}
    """.toStream

    val sample   = SampleData(data)
    val table    = fromSample(sample, Some(5))
    val results  = toJson(table.takeRange(5, 2))

    val expected = jsonMany"""
      {"key":[6],"value":"ack3"}
      {"key":[7],"value":"ack4"}
    """.toStream

    results.copoint must_== expected
  }

  def testTakeRangeFirstSliceOnly = {
    val data = jsonMany"""
      {"key":[1],"value":"foo"}
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack1"}
      {"key":[5],"value":"ack2"}
      {"key":[6],"value":"ack3"}
      {"key":[7],"value":"ack4"}
      {"key":[8],"value":"ack5"}
    """.toStream

    val sample  = SampleData(data)
    val table   = fromSample(sample, Some(5))
    val results = toJson(table.takeRange(0, 5))

    val expected = jsonMany"""
      {"key":[1],"value":"foo"}
      {"key":[2],"value":12}
      {"key":[3],"value":{"baz":true}}
      {"key":[4],"value":"ack1"}
      {"key":[5],"value":"ack2"}
    """.toStream

    results.copoint must_== expected
  }
}
