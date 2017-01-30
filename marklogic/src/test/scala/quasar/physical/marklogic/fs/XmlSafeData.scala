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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.Data
import quasar.DataArbitrary._

import scalaz._, Scalaz._
import quasar.pkg.tests._

/** Quasar Data that is known to be representable in XML. */
final case class XmlSafeData(data: Data) extends AnyVal

object XmlSafeData {
  implicit val arbitrary: Arbitrary[XmlSafeData] = genNested(
    genKey = Gen.nonEmptyListOf(Gen.alphaChar) ^^ (_.mkString),
    genAtomicData = genAtomicData(
      strSrc = genAlphaNumString,
      intSrc = genInt ^^ (x => BigInt(x)),
      decSrc = genDouble ^^ (x => BigDecimal(x)),
      idSrc  = genAlphaNumString
    )
  ) ^^ apply

  implicit val equal  = eqBy[XmlSafeData](_.data)
  implicit val show   = showBy[XmlSafeData](_.data)
  implicit val shrink = Shrink.xmap[Data, XmlSafeData](apply, _.data)
}
