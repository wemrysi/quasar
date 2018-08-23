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

import quasar.pkg.tests._

import scalaz.{Equal, IList, NonEmptyList}

trait DimensionsGenerator {
  implicit def arbitraryDimensions[A: Arbitrary: Equal]: Arbitrary[Dimensions[A]] =
    Arbitrary(for {
      unionSize <- Gen.choose(0, 10)
      union <- Gen.listOfN(unionSize, genJoin[A])
    } yield Dimensions.normalize(Dimensions(IList.fromList(union))))

  private def genJoin[A: Arbitrary]: Gen[NonEmptyList[A]] =
    for {
      size <- Gen.choose(0, 9)
      h <- arbitrary[A]
      t <- Gen.listOfN(size, arbitrary[A])
    } yield NonEmptyList.nel(h, IList.fromList(t))
}

object DimensionsGenerator extends DimensionsGenerator
