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

final case class TableRep[T](table: T, companion: TableMethodsCompanion[T]) {
  implicit def tableMethods(table: T): TableMethods[T] = companion tableMethods table
}

trait Table extends TableMethods[Table] {
  def self: Table            = this
  def companion: Table.type  = Table
}
object Table extends TableMethodsCompanion[Table] {
  def fromSlices(slices: NeedSlices, size: TableSize): Table   = new ExternalTable(slices, size)
  implicit def tableMethods(table: Table): TableMethods[Table] = table
}
