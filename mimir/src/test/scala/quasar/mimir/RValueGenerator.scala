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

package quasar.mimir

import quasar.precog.common.RValue
import quasar.yggdrasil.SValueGenerators

import scala.math

import org.scalacheck.{Arbitrary, Gen}

// TODO: This should probably live over in blueeyes, but defining here for
//       now as it depends on Gen[SValue] from yggdrasil.
trait RValueGenerator {
  implicit val rValueArbitrary: Arbitrary[RValue] = {
    val svgen = new SValueGenerators {}

    Arbitrary(for {
      size <- Gen.size
      depth = math.round(math.log(size.toDouble)).toInt
      svalue <- svgen.svalue(depth)
    } yield svalue.toRValue)
  }
}

object RValueGenerator extends RValueGenerator
