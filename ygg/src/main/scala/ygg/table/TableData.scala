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

import ygg._, common._, json._
import trans._
import scalaz._

final case class TableRep[T](table: T, companion: TableMethodsCompanion[T]) {
  implicit def tableMethods(table: T): TableMethods[T] = companion tableMethods table
}

sealed trait TableData {
  def size: TableSize
  def slices: StreamT[Need, Slice]
  def projections: Map[Path, Projection]

  def self: TableData           = this
  def companion: TableData.type = TableData
}

object TableData extends TableMethodsCompanion[TableData] {
  override def empty: TableData                                        = new Internal(Slice.empty)
  def fromSlices(slices: NeedSlices, size: TableSize): TableData       = new External(slices, size)
  implicit def tableMethods(table: TableData): TableMethods[TableData] = new Impl(table)

  private type V           = JValue
  private type T           = TableData
  private type TS          = Seq[T]
  private type F1          = TransSpec1
  private type F2          = TransSpec2
  private type LazySeq[+A] = Stream[A]
  private type ProjMap     = Map[Path, Projection]

  final case class External(slices: NeedSlices, size: TableSize) extends TableData {
    def projections = Map()
  }
  final case class Internal(slice: Slice) extends TableData {
    def slices      = singleStreamT(slice)
    def size        = ExactSize(slice.size)
    def projections = Map()
  }
  final case class Projs(underlying: TableData, proj: Map[Path, Projection]) extends TableData {
    def slices      = underlying.slices
    def size        = underlying.size
    def projections = underlying.projections ++ proj
  }

  class BinaryTableData(left: TableData, right: TableData) {
    def cogroup(left: PairOf[F1], right: PairOf[F1], both: F2): T = ???
    def concat(): T                                               = ???
    def cross(spec: F2): T                                        = ???
    def zip(): T                                                  = ???
  }

  class Impl(val self: TableData) extends TableMethods[TableData] {
    def companion = TableData

    def size: TableSize                    = self.size
    def slices: StreamT[Need, Slice]       = self.slices
    def projections: Map[Path, Projection] = self.projections
    def withProjections(proj: ProjMap): T  = Projs(self, proj)
  }
}
