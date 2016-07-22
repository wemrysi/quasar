/*
 * Copyright 2009-2010 WorldWide Conferencing, LLC
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
  import blueeyes._, json._

  object JsonTestSupport extends quasar.precog.TestSupport with ArbitraryJValue
}

package blueeyes.json {
  import org.scalacheck._
  import Gen._
  import Arbitrary.arbitrary
  import quasar.precog.TestSupport._

  trait ArbitraryJValue {
    def genJValue: Gen[JValue] = frequency(
      5 -> genSimple,
      1 -> delay(genJArray),
      1 -> delay(genJObject)
    )

    def genSimple: Gen[JValue]   = oneOf[JValue](JNull, genJNum, genJBool, genJString)
    def genSmallInt: Gen[Int]    = choose(0, 5)
    def genJNum: Gen[JNum]       = genBigDecimal ^^ (x => JNum(x))
    def genJBool: Gen[JBool]     = genBool ^^ (x => JBool(x))
    def genJString: Gen[JString] = alphaStr ^^ (s => JString(s))
    def genJField: Gen[JField]   = alphaStr >> (name => genJValue >> (value => genPosInt >> (id => JField(s"$name$id", value))))
    def genJObject: Gen[JObject] = genSmallInt >> genJFieldList ^^ (xs => JObject(xs.toMap))
    def genJArray: Gen[JArray]   = genSmallInt >> genJList ^^ JArray

    def genJList(size: Int): Gen[List[JValue]]      = listOfN(size, genJValue)
    def genJFieldList(size: Int): Gen[List[JField]] = listOfN(size, genJField)

    implicit def arbJValue: Arbitrary[JValue]   = Arbitrary(genJValue)
    implicit def arbJObject: Arbitrary[JObject] = Arbitrary(genJObject)

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
