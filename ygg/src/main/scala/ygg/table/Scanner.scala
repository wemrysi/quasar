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

package ygg.table

import ygg._, common._

trait Scanner {
  type A
  def init: A
  def scan(a: A, cols: ColumnMap, range: Range): A -> ColumnMap
}
object Scanner {
  def apply[A0](initValue: A0)(f: (A0, ColumnMap, Range) => (A0 -> ColumnMap)) = new Scanner {
    type A = A0
    def init: A = initValue
    def scan(a: A, cols: ColumnMap, range: Range) = f(a, cols, range)
  }
}

trait CReducer[A] {
  def reduce(schema: CSchema, range: Range): A
}
