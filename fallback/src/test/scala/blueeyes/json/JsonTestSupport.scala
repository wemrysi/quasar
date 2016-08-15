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

import blueeyes._, json._
import quasar.precog.TestSupport._
import Gen._

package quasar.precog {
  object JsonTestSupport extends TestSupport with JsonGenerators {
    def arb[A](implicit z: Arbitrary[A]): Arbitrary[A] = z

    implicit def arbJValue: Arbitrary[JValue]   = Arbitrary(genJValue)
    implicit def arbJObject: Arbitrary[JObject] = Arbitrary(genJObject)
    implicit def arbJPath: Arbitrary[JPath]     = Arbitrary(genJPath)
  }
}

package blueeyes.json {
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

    def genJPathNode: Gen[JPathNode] = frequency(
      1 -> (choose(0, 10) ^^ JPathIndex),
      9 -> (genIdent ^^ JPathField)
    )
    def genJPath: Gen[JPath] = genJPathNode * choose(0, 10) ^^ (JPath(_))

    /** The delay wrappers are needed because we generate
     *  JValues recursively.
     */
    def genJValue: Gen[JValue] = frequency(
      5 -> genSimple,
      1 -> delay(genJArray),
      1 -> delay(genJObject)
    )

    def genIdent: Gen[String]    = alphaLowerChar * choose(3, 8) ^^ (_.mkString)
    def genSimple: Gen[JValue]   = oneOf[JValue](JNull, genJNum, genJBool, genJString)
    def genSmallInt: Gen[Int]    = choose(0, 5)
    def genJNum: Gen[JNum]       = genBigDecimal ^^ (x => JNum(x))
    def genJBool: Gen[JBool]     = genBool ^^ (x => JBool(x))
    def genJString: Gen[JString] = genIdent ^^ (s => JString(s))
    def genJField: Gen[JField]   = (genIdent, genPosInt, genJValue) >> ((name, id, value) => JField(s"$name$id", value))
    def genJObject: Gen[JObject] = genJField * genSmallInt ^^ (xs => JObject(xs: _*))
    def genJArray: Gen[JArray]   = genJValue * genSmallInt ^^ JArray
  }
}
