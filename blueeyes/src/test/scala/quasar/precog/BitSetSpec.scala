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
        val mod = math.abs(mod0) % 20;    // don't let scalacheck go crazy

        mod > 2 ==> {
          val offset = math.abs(offset0) % mod;

          val sparsened = bs.sparsenByMod(offset, mod)
          val bound = bs.length << 6

          (0 until bound) forall { i =>
            bs(i) mustEqual sparsened(i * mod + offset)
          }
        }
      }
    }
  }
}
