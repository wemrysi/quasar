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

package quasar.fs

import quasar.Predef._

import org.scalacheck.{Arbitrary, Gen}
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz.syntax.functor._

trait NumericArbitrary {
  import Arbitraries.genOption

  implicit val positiveArbitrary: Arbitrary[Positive] =
    Arbitrary(genOption(Gen.choose(1, Short.MaxValue.toLong) ∘ (Positive(_))))

  implicit val negativeArbitrary: Arbitrary[Negative] =
    Arbitrary(genOption(Gen.choose(Long.MinValue, -1) ∘ (Negative(_))))

  implicit val naturalArbitrary: Arbitrary[Natural] =
    Arbitrary(genOption(Gen.choose(0, Short.MaxValue.toLong) ∘ (Natural(_))))
}

object NumericArbitrary extends NumericArbitrary
