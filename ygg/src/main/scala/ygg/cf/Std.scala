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

package ygg.cf

import ygg.common._
import ygg.table._

object std {
  val Lt = CF2P("builtin::ct::lt") {
    case (c1: LongColumn, c2: LongColumn)     => new Map2ColumnJJZ(c1, c2)(_ < _)
    case (c1: DoubleColumn, c2: DoubleColumn) => new Map2ColumnDDZ(c1, c2)(_ < _)
    case (c1: NumColumn, c2: LongColumn)      => new Map2ColumnNLZ(c1, c2)(_ < _)
    case (c1: LongColumn, c2: NumColumn)      => new Map2ColumnLNZ(c1, c2)(_ < _)
    case (c1: NumColumn, c2: DoubleColumn)    => new Map2ColumnNDZ(c1, c2)(_ < _)
    case (c1: DoubleColumn, c2: NumColumn)    => new Map2ColumnDNZ(c1, c2)(_ < _)
    case (c1: NumColumn, c2: NumColumn)       => new Map2ColumnNNZ(c1, c2)(_ < _)
  }

  val Eq = CF2P("builtin::ct::eq") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2ColumnZZZ(c1, c2, _ == _)
    case (c1: LongColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: LongColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: LongColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = BigDecimal(c1(row)) == c2(row)
      }
    case (c1: DoubleColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: DoubleColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: DoubleColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = BigDecimal(c1(row)) == c2(row)
      }
    case (c1: NumColumn, c2: LongColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == BigDecimal(c2(row))
      }
    case (c1: NumColumn, c2: DoubleColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == BigDecimal(c2(row))
      }
    case (c1: NumColumn, c2: NumColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: StrColumn, c2: StrColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = c1(row) == c2(row)
      }
    case (c1: DateColumn, c2: DateColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = NumericComparisons.compare(c1(row), c2(row)) == 0
      }
    case (c1: EmptyObjectColumn, c2: EmptyObjectColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = true // always true where both columns are defined
      }
    case (c1: EmptyArrayColumn, c2: EmptyArrayColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = true // always true where both columns are defined
      }
    case (c1: NullColumn, c2: NullColumn) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = true // always true where both columns are defined
      }
    case (c1, c2) =>
      new Map2Column(c1, c2) with BoolColumn {
        def apply(row: Int) = false // equality is defined between all types
      }
  }
  val And = CF2P("builtin::ct::and") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2ColumnZZZ(c1, c2, _ && _)
  }
  val Or = CF2P("builtin::ct::or") {
    case (c1: BoolColumn, c2: BoolColumn) => new Map2ColumnZZZ(c1, c2, _ || _)
  }
}
// type Std
