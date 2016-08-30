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

package ygg.tests

import ygg._, common._, table._

class FNSpec extends quasar.Qspec {
  "function implementations" >> {
    "partials must work correctly" in {
      val col4 = Column.const(4L)
      val col2 = Column.const(2L)
      val col0 = Column.const(0L)
      val f2   = DivZeroLongP

      f2(col4, col2).forall(_.isDefinedAt(0)) must beTrue
      f2(col4, col0).forall(_.isDefinedAt(0)) must beFalse
      f2(col4, col2) must beLike { case Some(c: LongColumn) => c(0) must_== 2L }
    }
  }

  val AddOneLongP = CF1P("testing::ct::addOneLong") {
    case (c: LongColumn) =>
      new LongColumn {
        def isDefinedAt(row: Int) = c.isDefinedAt(row)
        def apply(row: Int)       = c(row) + 1
      }
  }

  val DivZeroLongP = CF2P("testing::ct::divzerolong") {
    case (c1: LongColumn, c2: LongColumn) =>
      new LongColumn {
        def isDefinedAt(row: Int) = c1.isDefinedAt(row) && c2.isDefinedAt(row) && c2(row) != 0
        def apply(row: Int)       = c1(row) / c2(row)
      }
  }
}
