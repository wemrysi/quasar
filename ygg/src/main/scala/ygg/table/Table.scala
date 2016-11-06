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
import trans._
import scalaz._, Scalaz._

final case class TableRep[T](table: T, convert: T => TableMethods[T], companion: TableMethodsCompanion[T]) {
  implicit def tableToMethods(table: T): TableMethods[T] = convert(table)
}

trait Table extends TableMethods[Table] {
  def asRep: TableRep[Table] = TableRep[Table](this, x => x, companion)
  def self: Table            = this
  def companion: Table.type  = Table
}
object Table extends TableMethodsCompanion[Table] {
  type M[+X] = Need[X]
  type T     = Table

  def fromSlice(slice: Slice): Table                         = new InternalTable(slice)
  def fromSlices(slices: NeedSlices, size: TableSize): Table = new ExternalTable(slices, size)
  def empty: Table                                           = Table(emptyStreamT(), ExactSize(0))

  implicit def tableMethods(table: Table): TableMethods[Table] = table

  def sort[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder): F[T]       = sortCommon[F](table, key, order, unique = false)
  def sortUnique[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder): F[T] = sortCommon[F](table, key, order, unique = true)

  private def sortCommon[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder, unique: Boolean): F[T] =
    groupByN[F](externalize(table), Seq(key), root, order, unique) map (_.headOption getOrElse empty)

  def merge(grouping: GroupingSpec[T])(body: (RValue, GroupId => M[T]) => M[T]): M[T] = MergeTable[T](grouping)(body)

  def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): Table = {
    val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)

    def makeSlice(data: Stream[RValue]): Slice -> Stream[RValue] =
      data splitAt sliceSize leftMap (Slice fromRValues _)

    fromSlices(
      unfoldStream(values)(events => Need(events.nonEmpty option makeSlice(events.toStream))),
      ExactSize(values.length)
    )
  }
}
