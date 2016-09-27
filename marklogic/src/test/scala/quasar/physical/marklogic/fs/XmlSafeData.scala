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

import org.scalacheck.{Arbitrary, Gen, Shrink}
import scalaz._, Scalaz._

/** Quasar Data that is known to be representable in XML. */
final case class XmlSafeData(data: Data) extends scala.AnyVal

object XmlSafeData {
  import Arbitrary.{arbitrary => arb}

  implicit val equal: Equal[XmlSafeData] =
    Equal.equalBy(_.data)

  implicit val show: Show[XmlSafeData] =
    Show[Data].contramap(_.data)

  implicit val arbitrary: Arbitrary[XmlSafeData] =
    Arbitrary(genData(
      objKeySrc = Gen.nonEmptyListOf(Gen.alphaChar) map (_.mkString   ),
      strSrc    = Gen.listOf(Gen.alphaNumChar)      map (_.mkString   ),
      intSrc    = arb[Int]                          map (BigInt(_)    ),
      decSrc    = arb[Double]                       map (BigDecimal(_)),
      idSrc     = Gen.listOf(Gen.alphaNumChar)      map (_.mkString   )
    ) map (XmlSafeData(_)))

  implicit val shrink: Shrink[XmlSafeData] =
    Shrink.xmap[Data, XmlSafeData](XmlSafeData(_), _.data)
}
