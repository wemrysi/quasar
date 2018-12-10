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

package quasar.qscript.rewrites

import quasar.Qspec
import quasar.qscript._

import matryoshka.data.Fix

object IsInnerExprSpec extends Qspec {
  import MapFuncsCore.IntLit

  val func = new construction.Func[Fix]

  def testBinary(f: (FreeMap[Fix], FreeMap[Fix]) => FreeMap[Fix]) = {
    val prjA = func.ProjectKeyS(func.Hole, "A")
    val prjB = func.ProjectKeyS(func.Hole, "B")
    val two = IntLit[Fix, Hole](2)

    // "independent"/"dependent" on Hole

    "(independent, independent)" >> {
      isInnerExpr(f(func.Now, two)) must beTrue
    }

    "(dependent, dependent)" >> {
      isInnerExpr(f(prjA, prjB)) must beTrue
    }

    "!(independent, dependent)" >> {
      isInnerExpr(f(two, prjA)) must beFalse
    }

    "!(dependent, independent)" >> {
      isInnerExpr(f(prjB, func.Now)) must beFalse
    }
  }

  "And" >> testBinary(func.And)

  "Or" >> testBinary(func.Or)

  "ConcatMaps" >> testBinary(func.ConcatMaps)

  "ConcatArrays" >> testBinary(func.ConcatArrays)

  "IfUndefined" >> {
    "independent" >> {
      isInnerExpr(func.IfUndefined(func.Now, IntLit(6))) must beTrue
    }

    "not dependent" >> {
      isInnerExpr(func.IfUndefined(func.ProjectKeyS(func.Hole, "foo"), IntLit(6))) must beFalse
    }
  }

  "Other" >> {
    "hole is inner" >> {
      isInnerExpr(func.Hole) must beTrue
    }

    "undefined-agnostic expressions are inner" >> {
      val expr =
        func.Multiply(
          func.Add(
            func.ProjectKeyS(func.ProjectIndexI(func.Hole, 4), "factor"),
            func.Length(func.ProjectIndexI(func.Hole, 2))),
          func.ExtractDayOfYear(func.Now))

      isInnerExpr(expr) must beTrue
    }
  }
}
