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

package quasar.precog

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.scalacheck._

object BitSetSpec extends Specification with ScalaCheck {

  implicit val arbBitSet: Arbitrary[BitSet] = {
    Arbitrary(Arbitrary.arbitrary[Array[Boolean]] map { bools =>
      val bs = new BitSet(bools.length)
      for (i <- 0 until bools.length) {
        bs(i) = bools(i)
      }
      bs
    })
  }

  "bitset (java)" should {
    "sparsenByMod" in prop { (bs: BitSet, offset0: Int, mod0: Int) =>
      (mod0 > Int.MinValue && offset0 > Int.MinValue) ==> {
        val mod = math.abs(mod0) % 20    // don't let scalacheck go crazy

        mod > 2 ==> {
          val offset = math.abs(offset0) % mod

          val sparsened = bs.sparsenByMod(offset, mod)
          val bound = bs.length << 6

          (0 until bound) forall { i =>
            bs(i) mustEqual sparsened(i * mod + offset)
          }
        }
      }
    }

    "setByMod" in prop { (bs: BitSet, mod0: Int) =>
      (mod0 > Int.MinValue) ==> {
        val mod = math.abs(mod0) % 20    // don't let scalacheck go crazy

        (mod > 0) ==> {
          bs.setByMod(mod)
          val bound = bs.length << 6

          (0 until (bound / mod) by mod) forall { i =>
            (0 until mod) must contain { j: Int =>
              if (i + j < bound)    // mod might not divide bound
                bs(i + j) must beTrue
              else
                ko
            }
          }
        }
      }
    }

    "setByMod avoid positive wrap-around masking issues" in {
      val bs = new BitSet(128)    // ensure we have two longs

      // 64 % 10 = 4, so we should wrap-around mid-mask
      bs.setByMod(10)

      // sanity check on first mod
      (0 until 10) must contain { i: Int =>
        bs(i) must beTrue
      }

      (60 until 70) must contain { i: Int =>
        bs(i) must beTrue
      }
    }

    // note that these negative tests assume that we're setting the mod zero bit
    // that's an implementation detail and technically not forced by the contract
    "setByMod avoid negative wrap-around masking issues" in {
      val bs = new BitSet(128)    // ensure we have two longs

      bs.set(64)    // first bit in the second long

      // 64 % 10 = 4, so we should wrap-around mid-mask
      bs.setByMod(10)

      bs(60) must beFalse
      bs(61) must beFalse
      bs(62) must beFalse
      bs(63) must beFalse
      bs(64) must beTrue
      bs(65) must beFalse
      bs(66) must beFalse
      bs(67) must beFalse
      bs(68) must beFalse
      bs(69) must beFalse
    }

    "setByMod avoid mask overlap on mod > 32" in {
      val bs = new BitSet(448)    // ensure we have seven longs

      bs.set(415)
      bs.set(416)

      // 64 % 35 = 29, and 64 * 6 = 384, and 384 % 35 = 0
      bs.setByMod(35)

      bs(35) must beTrue

      bs(350) must beTrue
      (351 to 384).map(bs(_)) must contain(beFalse).forall
    }
  }
}
