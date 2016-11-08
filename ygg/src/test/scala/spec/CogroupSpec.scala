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

import scalaz._, Scalaz._, Ordering._, Either3._
import ygg._, common._, json._, table._
import scala.Predef.identity

class CogroupSpec extends TableQspec {
  import SampleData._
  import trans._, constants._

  type CogroupResult[A] = Stream[Either3[A, (A, A), A]]
  implicit def cogroupData: Arbitrary[CogroupData] = Arbitrary(genCogroupData)

  "in cogroup" >> {
    "perform a trivial cogroup"                                                in testTrivialCogroup(identity[Table])
    "perform a simple cogroup"                                                 in testSimpleCogroup(identity[Table])
    "perform another simple cogroup"                                           in testAnotherSimpleCogroup
    "cogroup for unions"                                                       in testUnionCogroup
    "perform yet another simple cogroup"                                       in testAnotherSimpleCogroupSwitched
    "cogroup across slice boundaries"                                          in testCogroupSliceBoundaries
    "error on unsorted inputs"                                                 in testUnsortedInputs
    "cogroup partially defined inputs properly"                                in testPartialUndefinedCogroup

    "survive pathology 1"                                                      in testCogroupPathology1
    "survive pathology 2"                                                      in testCogroupPathology2
    "survive pathology 3"                                                      in testCogroupPathology3
    "survive scalacheck"                                                       in prop(testCogroup _)

    "not truncate cogroup when right side has long equal spans"                in testLongEqualSpansOnRight
    "not truncate cogroup when left side has long equal spans"                 in testLongEqualSpansOnLeft
    "not truncate cogroup when both sides have long equal spans"               in testLongEqualSpansOnBoth
    "not truncate cogroup when left side is long span and right is increasing" in testLongLeftSpanWithIncreasingRight
  }

  @tailrec private def computeCogroup[A](l: Stream[A], r: Stream[A], acc: CogroupResult[A])(implicit ord: Ord[A]): CogroupResult[A] = (l, r) match {
    case (Seq(), _)             => acc ++ (r map right3)
    case (_, Seq())             => acc ++ (l map left3)
    case (lh #:: lt, rh #:: rt) =>
    (lh ?|? rh) match {
        case EQ =>
          val (leftSpan, leftRemain)   = l partition (_ ?|? lh === EQ)
          val (rightSpan, rightRemain) = r partition (_ ?|? rh === EQ)
          val cartesian                = leftSpan flatMap (lv => rightSpan map (rv => middle3(lv -> rv)))
          computeCogroup(leftRemain, rightRemain, acc ++ cartesian)

        case LT =>
          val (leftRun, leftRemain) = l partition (_ ?|? rh == LT)
          computeCogroup(leftRemain, r, acc ++ (leftRun map left3))

        case GT =>
          val (rightRun, rightRemain) = r partition (lh ?|? _ == GT)
          computeCogroup(l, rightRemain, acc ++ (rightRun map right3))
      }
  }

  private def cogroupKV(left: Table, right: Table)(specs: TransSpec2*): Table = {
    left.cogroup(`.` \ "key", `.` \ "key", right)(
      `.`,
      `.`,
      OuterObjectConcat(
        wrapFieldLeft("key"),
        OuterObjectConcat(specs: _*)
      )
    )
  }

  private def testCogroup(pair: CogroupData) = {
    val (l, r)   = pair
    val ltable   = fromSample(l)
    val rtable   = fromSample(r)
    val keyOrder = Ord[JValue].contramap((_: JValue) \ "key")

    val expected = computeCogroup(l.data, r.data, Stream())(keyOrder) map {
      case Left3(jv)           => jv
      case Middle3((jv1, jv2)) => jobject("key" -> (jv1 \ "key"), "valueLeft" -> (jv1 \ "value"), "valueRight" -> (jv2 \ "value"))
      case Right3(jv)          => jv
    }

    val result: Table = cogroupKV(ltable, rtable)(
      wrapFieldLeft("value", as = "valueLeft"),
      wrapFieldRight("value", as = "valueRight")
    )

    result.toSeq must_=== expected
  }

  private def testTrivialCogroup(f: Table => Table) = {
    def recl = toRecord(Array(0L), JArray(JNum(12) :: Nil))
    def recr = toRecord(Array(0L), JArray(JUndefined :: JNum(13) :: Nil))

    val ltable   = fromSample(SampleData(Stream(recl)))
    val rtable   = fromSample(SampleData(Stream(recr)))
    val expected = Vector(toRecord(Array(0L), JArray(JNum(12) :: JUndefined :: JNum(13) :: Nil)))

    val result: Table = cogroupKV(ltable, rtable)(
      WrapObject(OuterArrayConcat(`<.` \ "value", SourceValue.Right), "value")
    )

    toJsonSeq(f(result)) must_=== expected
  }

  private def testSimpleCogroup(f: Table => Table) = {
    def recl(i: Long)    = toRecord(Array(i), json"""{ "left": ${i.toString} }""")
    def recr(i: Long)    = toRecord(Array(i), json"""{ "right": ${i.toString} }""")
    def recBoth(i: Long) = toRecord(Array(i), json"""{ "left": ${i.toString}, "right": ${i.toString} }""")

    val ltable = fromSample(SampleData(Stream(recl(0), recl(1), recl(3), recl(3), recl(5), recl(7), recl(8), recl(8))))
    val rtable = fromSample(SampleData(Stream(recr(0), recr(2), recr(3), recr(4), recr(5), recr(5), recr(6), recr(8), recr(8))))

    val expected = Vector(
      recBoth(0),
      recl(1),
      recr(2),
      recBoth(3),
      recBoth(3),
      recr(4),
      recBoth(5),
      recBoth(5),
      recr(6),
      recl(7),
      recBoth(8),
      recBoth(8),
      recBoth(8),
      recBoth(8)
    )

    val result: Table = cogroupKV(ltable, rtable)(wrapFieldBoth("value"))

    toJsonSeq(f(result)) must_=== expected
  }

  private def testUnionCogroup = {
    def recl(i: Long, j: Long) = toRecord(Array(i), JObject(List(JField("left", JNum(j)))))
    def recr(i: Long, j: Long) = toRecord(Array(i), JObject(List(JField("right", JNum(j)))))

    val ltable = fromSample(SampleData(Stream(recl(0, 1), recl(1, 12), recl(3, 13), recl(4, 42), recl(5, 77))))
    val rtable = fromSample(SampleData(Stream(recr(6, -1), recr(7, 0), recr(8, 14), recr(9, 42), recr(10, 77))))

    val expected = Vector(
      recl(0, 1),
      recl(1, 12),
      recl(3, 13),
      recl(4, 42),
      recl(5, 77),
      recr(6, -1),
      recr(7, 0),
      recr(8, 14),
      recr(9, 42),
      recr(10, 77)
    )

    val result: Table = cogroupKV(ltable, rtable)(wrapFieldBoth("value"))
    toJsonSeq(result) must_=== expected
  }

  private def testAnotherSimpleCogroup = {
    def recl(i: Long)    = toRecord(Array(i), jobject("left" -> JString(i.toString)))
    def recr(i: Long)    = toRecord(Array(i), jobject("right" ->  JString(i.toString)))
    def recBoth(i: Long) = toRecord(Array(i), jobject("left" -> JString(i.toString), "right" -> JString(i.toString)))

    val ltable = fromSample(SampleData(Stream(recl(2), recl(3), recl(4), recl(6), recl(7))))
    val rtable = fromSample(SampleData(Stream(recr(0), recr(1), recr(5), recr(6), recr(7))))

    val expected = Vector(
      recr(0),
      recr(1),
      recl(2),
      recl(3),
      recl(4),
      recr(5),
      recBoth(6),
      recBoth(7)
    )

    val result: Table = cogroupKV(ltable, rtable)(wrapFieldBoth("value"))
    toJsonSeq(result) must_=== expected
  }

  private def testAnotherSimpleCogroupSwitched = {
    def recl(i: Long)    = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Long)    = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))
    def recBoth(i: Long) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)), JField("right", JString(i.toString)))))

    val rtable = fromSample(SampleData(Stream(recr(2), recr(3), recr(4), recr(6), recr(7))))
    val ltable = fromSample(SampleData(Stream(recl(0), recl(1), recl(5), recl(6), recl(7))))

    val expected = Vector(
      recl(0),
      recl(1),
      recr(2),
      recr(3),
      recr(4),
      recl(5),
      recBoth(6),
      recBoth(7)
    )

    val result: Table = cogroupKV(ltable, rtable)(wrapFieldBoth("value"))
    toJsonSeq(result) must_=== expected
  }

  private def testUnsortedInputs = {
    def recl(i: Long) = toRecord(Array(i), JObject(List(JField("left", JString(i.toString)))))
    def recr(i: Long) = toRecord(Array(i), JObject(List(JField("right", JString(i.toString)))))

    val ltable = fromSample(SampleData(Stream(recl(0), recl(1))))
    val rtable = fromSample(SampleData(Stream(recr(1), recr(0))))

    def result: Table = cogroupKV(ltable, rtable)(wrapFieldBoth("value"))

    result.toVector must throwAn[Exception]
  }

  private def testCogroupPathology1 = {
    val s1 = SampleData(Stream(toRecord(Array(1, 1, 1), json"""{ "a":[] }""")))
    val s2 = SampleData(Stream(toRecord(Array(1, 1, 1), json"""{ "b":0 }""")))

    testCogroup(s1 -> s2)
  }

  private def testCogroupSliceBoundaries = {
    val s1 = SampleData(
      Stream(
        toRecord(Array(1), json"""{ "ruoh5A25Jaxa":-1.0 }"""),
        toRecord(Array(2), json"""{ "ruoh5A25Jaxa":-2.735023101944097E37 }"""),
        toRecord(Array(3), json"""{ "ruoh5A25Jaxa":2.12274644226519E38 }"""),
        toRecord(Array(4), json"""{ "ruoh5A25Jaxa":1.085656944502855E38 }"""),
        toRecord(Array(5), json"""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }"""),
        toRecord(Array(6), json"""{ "ruoh5A25Jaxa":-1.0 }"""),
        toRecord(Array(7), json"""{ "ruoh5A25Jaxa":-3.4028234663852886E38 }"""),
        toRecord(Array(8), json"""{ "ruoh5A25Jaxa":2.4225587899613125E38 }"""),
        toRecord(Array(9), json"""{ "ruoh5A25Jaxa":-3.078101074510345E38 }"""),
        toRecord(Array(10), json"""{ "ruoh5A25Jaxa":0.0 }"""),
        toRecord(Array(11), json"""{ "ruoh5A25Jaxa":-2.049657967962047E38 }""")
      ))

    val s2 = SampleData(
      Stream(
        toRecord(Array(1), json"""{ "mbsn8ya":-629648309198725501 }"""),
        toRecord(Array(2), json"""{ "mbsn8ya":-1642079669762657762 }"""),
        toRecord(Array(3), json"""{ "mbsn8ya":-75462980385303464 }"""),
        toRecord(Array(4), json"""{ "mbsn8ya":-4407493923710190330 }"""),
        toRecord(Array(5), json"""{ "mbsn8ya":4611686018427387903 }"""),
        toRecord(Array(6), json"""{ "mbsn8ya":-4374327062386862583 }"""),
        toRecord(Array(7), json"""{ "mbsn8ya":1920642186250198767 }"""),
        toRecord(Array(8), json"""{ "mbsn8ya":1 }"""),
        toRecord(Array(9), json"""{ "mbsn8ya":0 }"""),
        toRecord(Array(10), json"""{ "mbsn8ya":1 }"""),
        toRecord(Array(11), json"""{ "mbsn8ya":758880641626989193 }""")
      ))

    testCogroup(s1 -> s2)
  }

  private def testCogroupPathology2 = {
    val s1 = SampleData(
      Stream(
        toRecord(Array(19, 49, 71), JArray(JNum(-4611686018427387904l) :: Nil)),
        toRecord(Array(28, 15, 27), JArray(JNum(-4611686018427387904l) :: Nil)),
        toRecord(Array(33, 11, 79), JArray(JNum(-1330862996622233403l) :: Nil)),
        toRecord(Array(38, 9, 3), JArray(JNum(483746605685223474l) :: Nil)),
        toRecord(Array(44, 75, 87), JArray(JNum(4611686018427387903l) :: Nil)),
        toRecord(Array(46, 47, 10), JArray(JNum(-4611686018427387904l) :: Nil)),
        toRecord(Array(47, 17, 78), JArray(JNum(3385965380985908250l) :: Nil)),
        toRecord(Array(47, 89, 84), JArray(JNum(-3713232335731560170l) :: Nil)),
        toRecord(Array(48, 47, 76), JArray(JNum(4611686018427387903l) :: Nil)),
        toRecord(Array(49, 66, 33), JArray(JNum(-1592288472435607010l) :: Nil)),
        toRecord(Array(50, 9, 89), JArray(JNum(-3610518022153967388l) :: Nil)),
        toRecord(Array(59, 54, 72), JArray(JNum(4178019033671378504l) :: Nil)),
        toRecord(Array(59, 80, 38), JArray(JNum(0) :: Nil)),
        toRecord(Array(61, 59, 15), JArray(JNum(1056424478602208129l) :: Nil)),
        toRecord(Array(65, 34, 89), JArray(JNum(4611686018427387903l) :: Nil)),
        toRecord(Array(73, 52, 67), JArray(JNum(-4611686018427387904l) :: Nil)),
        toRecord(Array(74, 60, 85), JArray(JNum(-4477191148386604184l) :: Nil)),
        toRecord(Array(76, 41, 86), JArray(JNum(-2686421995147680512l) :: Nil)),
        toRecord(Array(77, 46, 75), JArray(JNum(-1) :: Nil)),
        toRecord(Array(77, 65, 58), JArray(JNum(-4032275398385636682l) :: Nil)),
        toRecord(Array(86, 50, 9), JArray(JNum(4163435383002324073l) :: Nil))
      ))

    val s2 = SampleData(
      Stream(
        toRecord(Array(19, 49, 71), JArray(JUndefined :: JNum(2.2447601450142614E38) :: Nil)),
        toRecord(Array(28, 15, 27), JArray(JUndefined :: JNum(-1.0) :: Nil)),
        toRecord(Array(33, 11, 79), JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil)),
        toRecord(Array(38, 9, 3), JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
        toRecord(Array(44, 75, 87), JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
        toRecord(Array(46, 47, 10), JArray(JUndefined :: JNum(-7.090379511750481E37) :: Nil)),
        toRecord(Array(47, 17, 78), JArray(JUndefined :: JNum(2.646265046453461E38) :: Nil)),
        toRecord(Array(47, 89, 84), JArray(JUndefined :: JNum(0.0) :: Nil)),
        toRecord(Array(48, 47, 76), JArray(JUndefined :: JNum(1.3605700991092947E38) :: Nil)),
        toRecord(Array(49, 66, 33), JArray(JUndefined :: JNum(-1.4787158449349019E38) :: Nil)),
        toRecord(Array(50, 9, 89), JArray(JUndefined :: JNum(-1.0) :: Nil)),
        toRecord(Array(59, 54, 72), JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil)),
        toRecord(Array(59, 80, 38), JArray(JUndefined :: JNum(8.51654525599509E37) :: Nil)),
        toRecord(Array(61, 59, 15), JArray(JUndefined :: JNum(3.4028234663852886E38) :: Nil)),
        toRecord(Array(65, 34, 89), JArray(JUndefined :: JNum(-1.0) :: Nil)),
        toRecord(Array(73, 52, 67), JArray(JUndefined :: JNum(5.692401753312787E37) :: Nil)),
        toRecord(Array(74, 60, 85), JArray(JUndefined :: JNum(2.5390881291535566E38) :: Nil)),
        toRecord(Array(76, 41, 86), JArray(JUndefined :: JNum(-6.05866505535721E37) :: Nil)),
        toRecord(Array(77, 46, 75), JArray(JUndefined :: JNum(0.0) :: Nil)),
        toRecord(Array(77, 65, 58), JArray(JUndefined :: JNum(1.0) :: Nil)),
        toRecord(Array(86, 50, 9), JArray(JUndefined :: JNum(-3.4028234663852886E38) :: Nil))
      ))

    testCogroup(s1 -> s2)
  }

  private def testCogroupPathology3 = {
    val s1 = SampleData(jsonMany"""
      { "value":{ "ugsrry":3.0961191760668197E+307 }, "key":[2.0] }
      { "value":{ "ugsrry":0.0 }, "key":[3.0] }
      { "value":{ "ugsrry":3.323617580854415E+307 }, "key":[5.0] }
      { "value":{ "ugsrry":-9.458984438931391E+306 }, "key":[6.0] }
      { "value":{ "ugsrry":1.0 }, "key":[10.0] }
      { "value":{ "ugsrry":0.0 }, "key":[13.0] }
      { "value":{ "ugsrry":-3.8439741460685273E+307 }, "key":[14.0] }
      { "value":{ "ugsrry":5.690895589711475E+307 }, "key":[15.0] }
      { "value":{ "ugsrry":0.0 }, "key":[16.0] }
      { "value":{ "ugsrry":-5.567237049482096E+307 }, "key":[17.0] }
      { "value":{ "ugsrry":-8.988465674311579E+307 }, "key":[18.0] }
      { "value":{ "ugsrry":2.5882896341488965E+307 }, "key":[22.0] }
    """.toStream)

    val s2 = SampleData(jsonMany"""
      { "value":{ "fzqJh5csbfsZqgkoi":[-1E-40146] }, "key":[2.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[-9.44770762864723688E-39073] }, "key":[3.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[2.894611552200768372E+19] }, "key":[5.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[-2.561276432629787073E-42575] }, "key":[6.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[-1E-10449] }, "key":[10.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[2110233717777347493] }, "key":[13.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[3.039020270015831847E+19] }, "key":[14.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[1E-50000] }, "key":[15.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[-1.296393752892965818E-49982] }, "key":[16.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[4.611686018427387903E+50018] }, "key":[17.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[0E+48881] }, "key":[18.0] }
      { "value":{ "fzqJh5csbfsZqgkoi":[2.326724524858976798E-10633] }, "key":[22.0] }
    """.toStream)

    testCogroup(s1 -> s2)
  }

  private def testPartialUndefinedCogroup = {
    val ltable = fromJson(
      jsonMany"""
        { "key" : "foo", "val" : 4 }
      """
    )
    val rtable = fromJson(
      jsonMany"""
        { "key" : "foo", "val" : 2 }
        { "val" : 3 }
        { "key" : "foo", "val" : 4 }
      """
    )
    val expected = jsonMany"""
      { "key": "foo", "left": 4, "right": 2 }
      { "key": "foo", "left": 4, "right": 4 }
    """

    val result = cogroupKV(ltable, rtable)(
      wrapFieldLeft("val", as = "left"),
      wrapFieldRight("val", as = "right")
    )
    toJsonSeq(result) must_=== expected
  }

  private def testLongEqualSpansOnRight = {
    val record   = json"""{"key":"Bob","value":42}"""
    val ltable   = fromSample(SampleData(Stream(record)))
    val rtable   = fromSample(SampleData(Stream.tabulate(22)(i => json"""{"key":"Bob","value":$i}""")))
    val expected = Stream.tabulate(22)(JNum(_))

    val result: Table = ltable.cogroup(`.` \ "key", `.` \ "key", rtable)(
      WrapObject(`.`, "blah!"),
      WrapObject(`.`, "argh!"),
      `.>` \ "value"
    )

    toJsonSeq(result) must_=== expected
  }

  private def testLongEqualSpansOnLeft = {
    val record   = json"""{"key":"Bob","value":42}"""
    val ltable   = fromSample(SampleData(Stream.tabulate(22)(i => json"""{"key":"Bob","value":$i}""")))
    val rtable   = fromSample(SampleData(Stream(record)))
    val expected = Stream.tabulate(22)(JNum(_))

    val result: Table = ltable.cogroup(`.` \ "key", `.` \ "key", rtable)(
      WrapObject(`.`, "blah!"),
      WrapObject(`.`, "argh!"),
      `<.` \ "value"
    )

    toJsonSeq(result) must_=== expected
  }

  private def testLongEqualSpansOnBoth = {
    val table    = fromSample(SampleData(Stream.tabulate(22)(i => json"""{"key":"Bob","value":$i}""")))
    val expected = ( for (l  <- 0 until 22; r <- 0 until 22) yield json"""{ "left": $l, "right": $r }""" ).toStream

    val result: Table = table.cogroup(`.` \ "key", `.` \ "key", table)(
      WrapObject(`.`, "blah!"),
      WrapObject(`.`, "argh!"),
      InnerObjectConcat(
        WrapObject(`.>` \ "value", "right"),
        WrapObject(`<.` \ "value", "left")
      )
    )

    toJsonSeq(result) must_=== expected
  }

  private def testLongLeftSpanWithIncreasingRight = {
    val ltable   = fromJson(Stream.tabulate(12)(i => json"""{ "key": "Bob", "value": $i }"""))
    val rtable   = fromJson(jsonMany"""{"key":"Bob", "value":50} {"key":"Charlie", "value":60}""")
    val expected = Seq.tabulate(12)(i => json"""{ "left": $i, "right": 50 }""") :+ json"""{ "right": 60 }"""

    val result: Table = ltable.cogroup(`.` \ "key", `.` \ "key", rtable)(
      WrapObject(`.` \ "value", "left"),
      WrapObject(`.` \ "value", "right"),
      InnerObjectConcat(
        WrapObject(`.>` \ "value", "right"),
        WrapObject(`<.` \ "value", "left")
      )
    )

    toJsonSeq(result) must_=== expected
  }
}
