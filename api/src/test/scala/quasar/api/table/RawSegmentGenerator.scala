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

package quasar.api.table

import slamdata.Predef._

import org.scalacheck.{Arbitrary, Gen}, Arbitrary.arbitrary

trait RawSegmentGenerator {
  implicit val arbitrarySegmentFormat: Arbitrary[SegmentFormat] = {
    val genCsv = for {
      rec <- arbitrary[Byte]
      oqt <- arbitrary[Byte]
      cqt <- arbitrary[Byte]
      esc <- arbitrary[Byte]
    } yield SegmentFormat.Csv(rec, oqt, cqt, esc)

    Arbitrary(Gen.oneOf(
      Gen.const(SegmentFormat.Json),
      genCsv))
  }

  /** NB: Does not generate semantic data. */
  implicit val arbitraryRawSegment: Arbitrary[RawSegment] =
    Arbitrary(for {
      c <- Gen.sized(Gen.listOfN(_, arbitrary[Either[Int, String]]))
      f <- arbitrary[SegmentFormat]
      d <- arbitrary[Array[Byte]]
    } yield RawSegment(c, f, d))
}

object RawSegmentGenerator extends RawSegmentGenerator
