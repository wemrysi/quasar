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

import slamdata.Predef.{Array, Left, List, Right}

import cats.syntax.eq._

import org.specs2.ScalaCheck
import org.specs2.mutable.Specification

import scodec.{Attempt, DecodeResult}
import scodec.bits.BitVector

object RawSegmentSpec extends Specification with ScalaCheck with RawSegmentGenerator {

  "binary codec" should {
    "roudtrip arbitrary segments" >> prop { seg: RawSegment =>
      val encoded = RawSegment.binaryCodec.encode(seg)

      encoded.isSuccessful must beTrue

      val Attempt.Successful(DecodeResult(decoded, remainder)) =
        RawSegment.binaryCodec.decode(encoded.require)

      remainder must_=== BitVector.empty
      decoded eqv seg
    }

    "unboxed encode equivalent to codec" >> prop { seg: RawSegment =>
      val encoded = RawSegment.encode(seg.context, seg.format, seg.data)
      encoded must_=== RawSegment.binaryCodec.encode(seg).require
    }

    "format" >> {
      "use 32-bit context len + 4-bit format flag + 64-bit data length at minimum" >> {
        val encoded = RawSegment.encode(List(), SegmentFormat.Json, Array())
        encoded.size must_=== (32 + 4 + 64)
      }

      "min size bound is accurate" >> {
        RawSegment.binaryCodec.sizeBound.lowerBound must_=== (32 + 4 + 64)
      }

      "use 4-bits + int32 for index" >> {
        val encoded = RawSegment.encode(List(Left(5)), SegmentFormat.Json, Array())
        // [ctxLen ctxFlag int32 fmtFlag dataLen]
        encoded.size must_=== (32 + 4 + 32 + 4 + 64)
      }

      "use 4-bits + utf8_32 for field" >> {
        val encoded = RawSegment.encode(List(Right("aaaaa")), SegmentFormat.Json, Array())
        // [ctxLen ctxFlag strLen strBytes fmtFlag dataLen]
        encoded.size must_=== (32 + 4 + 32 + 40 + 4 + 64)
      }

      "use 32 bits to encode csv config" >> {
        val encoded = RawSegment.encode(List(), SegmentFormat.Csv(',', '"', '"', '\\'), Array())
        // [ctxLen fmtFlag csvCfg dataLen]
        encoded.size must_=== (32 + 4 + 32 + 64)
      }
    }
  }
}
