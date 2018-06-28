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

package quasar.precog {
  import quasar.blueeyes.json._
  import quasar.pkg.tests._

  object JsonTestSupport extends JsonGenerators {
    implicit def arbJValue: Arbitrary[JValue]   = Arbitrary(genJValue)
    implicit def arbJObject: Arbitrary[JObject] = Arbitrary(genJObject)
    implicit def arbJPath: Arbitrary[JPath]     = Arbitrary(genJPath)

    // BigDecimal *isn't* arbitrary precision!  AWESOME!!!
    implicit def arbBigDecimal: Arbitrary[BigDecimal] =
      Arbitrary(for {
        mantissa <- arbitrary[Long]
        exponent <- arbitrary[Int]

        adjusted = if (exponent.toLong + mantissa.toString.length >= Int.MaxValue.toLong)
          exponent - mantissa.toString.length
        else if (exponent.toLong - mantissa.toString.length <= Int.MinValue.toLong)
          exponent + mantissa.toString.length
        else
          exponent
      } yield BigDecimal(mantissa, adjusted, java.math.MathContext.UNLIMITED))
  }
}

package quasar.blueeyes.json {
  import quasar.pkg.tests._

  final case class JValueAndPath(jv: JValue, p: JPath)

  trait JsonGenerators {
    def badPath(jv: JValue, p: JPath): Boolean = (jv, p.nodes) match {
      case (JArray(_), JPathField(_) :: _)   => true
      case (JObject(_), JPathIndex(_) :: _)  => true
      case (_, Nil)                          => false
      case (_, JPathField(name) :: xs)       => badPath(jv \ name, JPath(xs))
      case (JArray(ns), JPathIndex(i) :: xs) => (i > ns.length) || (i < ns.length && badPath(ns(i), JPath(xs))) || badPath(JArray(Nil), JPath(xs))
      case (_, JPathIndex(i) :: xs)          => (i != 0) || badPath(JArray(Nil), JPath(xs))
    }

    def genJPathNode: Gen[JPathNode] = Gen.frequency(
      1 -> (choose(0, 10) ^^ JPathIndex),
      9 -> (genIdent ^^ JPathField)
    )
    def genJPath: Gen[JPath] = choose(0, 10) >> (len => Gen.listOfN(len, genJPathNode) ^^ (JPath(_)))
    def genJValue: Gen[JValue] = Gen.frequency(
      5 -> genSimple,
      1 -> Gen.delay(genJArray),
      1 -> Gen.delay(genJObject)
    )

    def genIdent: Gen[String]    = choose(3, 8) >> (len => arrayOfN(len, Gen.alphaLowerChar) ^^ (new String(_)))
    def genSimple: Gen[JValue]   = Gen.oneOf[JValue](JNull, genJNum, genJBool, genJString)
    def genSmallInt: Gen[Int]    = choose(0, 5)
    def genJNum: Gen[JNum]       = genBigDecimal ^^ (x => JNum(x))
    def genJBool: Gen[JBool]     = genBool ^^ (x => JBool(x))
    def genJString: Gen[JString] = genIdent ^^ (s => JString(s))
    def genJField: Gen[JField]   = genIdent >> (name => genJValue >> (value => genPosInt >> (id => JField(s"$name$id", value))))
    def genJObject: Gen[JObject] = genSmallInt >> genJFieldList ^^ (xs => JObject(xs: _*))
    def genJArray: Gen[JArray]   = genSmallInt >> genJList ^^ JArray

    def genJList(size: Int): Gen[List[JValue]]      = Gen.listOfN(size, genJValue)
    def genJFieldList(size: Int): Gen[List[JField]] = Gen.listOfN(size, genJField)
  }
}
