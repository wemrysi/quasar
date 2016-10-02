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

package ygg.tests.pkg

import ygg.common._
import ygg.data._

trait TestsPackage extends quasar.pkg.TestsPackage {
  def genBitSet(size: Int): Gen[BitSet] = {
    genBool * size ^^ { bools =>
      doto(new BitSet) { bs =>
        bools.indices foreach { i =>
          if (bools(i))
            bs set i
        }
      }
    }
  }

  // BigDecimal *isn't* arbitrary precision!  AWESOME!!!
  // (and scalacheck's BigDecimal gen will overflow at random)
  override def genBigDecimal: Gen[BigDecimal] = genBigDecimal(genExponent = genBigDecExponent)
  def genBigDecExponent              = choose(-50000, 50000)
  def genBigDecimal(genExponent: Gen[Int]): Gen[BigDecimal] = (genLong, genExponent) >> { (mantissa, exponent) =>
    def adjusted = (
      if (exponent.toLong + mantissa.toString.length >= Int.MaxValue.toLong)
        exponent - mantissa.toString.length
      else if (exponent.toLong - mantissa.toString.length <= Int.MinValue.toLong)
        exponent + mantissa.toString.length
      else
        exponent
    )
    decimal(unscaledVal = mantissa, scale = adjusted)
  }

  def genBitSet: Gen[BitSet] = listOf(0 upTo 500) ^^ (Bits(_))

  def genSparseBitSet: Gen[Codec[BitSet] -> BitSet] = {
    (0 upTo 500) >> { size =>
      val codec = Codec.SparseBitSetCodec(size)
      if (size > 0)
        listOf(0 upTo size - 1) ^^ (bits => codec -> Bits(bits))
      else
        const(codec -> Bits())
    }
  }
  def genSparseRawBitSet: Gen[Codec[RawBitSet] -> RawBitSet] = {
    (0 upTo 500) >> { size =>
      val codec: Gen[Codec[RawBitSet]] = Codec.SparseRawBitSetCodec(size)
      val bits: Gen[RawBitSet] = size match {
        case 0 => const(RawBitSet create 0)
        case n => listOf(0 upTo size - 1) ^^ (bits => doto(RawBitSet create size)(bs => bits foreach (bs set _)))
      }
      (codec, bits).zip
    }
  }
}
