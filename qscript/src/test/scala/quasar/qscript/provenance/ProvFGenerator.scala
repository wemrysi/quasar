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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.contrib.matryoshka.PatternArbitrary
import quasar.pkg.tests._

import matryoshka.Delay
import scalaz._

trait ProvFGenerator {
  import ProvF._

  implicit def arbitraryProvF[D: Arbitrary, I: Arbitrary, S: Arbitrary]: Delay[Arbitrary, ProvF[D, I, S, ?]] =
    new PatternArbitrary[ProvF[D, I, S, ?]] {
      val O = new Optics[D, I, S]

      def leafGenerators[A] =
        NonEmptyList(
          1 -> Gen.delay(Gen.const(O.fresh[A]())),
          10 -> (arbitrary[(D, S)] ^^ (O.project[A](_))),
          10 -> (arbitrary[(D, S)] ^^ (O.inject[A](_))),
          10 -> (arbitrary[(I, S)] ^^ (O.inflate[A](_))))

      def branchGenerators[A: Arbitrary] =
        uniformly(
          arbitrary[(A, A)] ^^ (O.both[A](_)),
          arbitrary[(A, A)] ^^ (O.thenn[A](_)))
    }
}

object ProvFGenerator extends ProvFGenerator
