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

package quasar.fs.mount

import quasar.Predef.Map
import quasar.contrib.pathy.{APath, PathArbitrary}

// NB: Something in here is needed by scalacheck's Arbitrary defs
//     for scala collections.
import scala.Predef._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait MountingsConfigArbitrary {
  import MountConfigArbitrary._, PathArbitrary._

  private def scaleSize[A](gen: Gen[A]): Gen[A] =
    Gen.sized(size => Gen.resize(size/40 + 1, gen))

  implicit val mountingsConfigArbitrary: Arbitrary[MountingsConfig] =
    // NB: this is a map, and MountConfig contains a map, so if the size isn't
    // reigned in we get gigantic configs.
    Arbitrary(scaleSize(arbitrary[Map[APath, MountConfig]]) map (MountingsConfig(_)))
}

object MountingsConfigArbitrary extends MountingsConfigArbitrary
