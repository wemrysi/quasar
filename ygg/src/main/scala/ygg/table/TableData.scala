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

sealed trait TableData extends Product with Serializable {
  def self: TableData           = this
  def companion: TableData.type = TableData

  def short_s: String = this match {
    case TableData.External(slices, size)   => s"[${size.size_s}]"
    case TableData.Internal(slice)          => s"[Singleton: ${slice.size}]"
    case TableData.Projections(table, proj) => s"[Projection: ${table.size.size_s}/${proj.size}]"
  }
  override def toString = short_s + this.columns.column_s
}

object TableData extends TableMethodsCompanion[TableData] {
  private type V           = JValue
  private type T           = TableData
  private type Size        = TableSize
  private type TS          = Seq[T]
  private type F1          = TransSpec1
  private type F2          = TransSpec2
  private type LazySeq[+A] = Stream[A]

  final case class External(slices: NeedSlices, size: Size)  extends TableData
  final case class Internal(slice: Slice)                    extends TableData
  final case class Projections(underlying: T, proj: ProjMap) extends TableData

  override def empty: T                                  = new Internal(Slice.empty)
  def fromSlices(slices: NeedSlices, size: Size): T = new External(slices, size)
  def methodsOf(table: T): TableMethods[T]               = new Impl(table)

  def sizeOf(table: T): Size = table match {
    case External(_, size)          => size
    case Internal(slice)            => ExactSize(slice.size)
    case Projections(underlying, _) => underlying.size
  }
  def slicesOf(table: TableData): StreamT[Need, Slice] = table match {
    case External(slices, _)        => slices
    case Internal(slice)            => singleStreamT(slice)
    case Projections(underlying, _) => underlying.slices
  }
  def projectionsOf(table: TableData): Map[Path, Projection] = table match {
    case Projections(underlying, ps) => underlying.projections ++ ps
    case _                           => Map()
  }
  def withProjections(table: TableData, ps: ProjMap): TableData = Projections(table, ps)

  private class Impl(val self: TableData) extends TableMethods[TableData] {
    def companion = TableData
  }
}
