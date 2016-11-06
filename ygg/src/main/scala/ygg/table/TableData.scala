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

sealed trait TableData {
  type M[+X] = Need[X]

  def size: TableSize
  def slices: StreamT[M, Slice]
}
trait TableDataCompanion extends TableMethodsCompanion[TableData] {
  def empty: TableData                                                 = new TableData.Internal(Slice.empty)
  def fromSlices(slices: NeedSlices, size: TableSize): TableData       = new TableData.External(slices, size)
  implicit def tableMethods(table: TableData): TableMethods[TableData] = new TableData.Impl(table)
}

object TableData extends TableDataCompanion {
  private type V           = JValue
  private type T           = TableData
  private type TS          = Seq[T]
  private type F1          = TransSpec1
  private type F2          = TransSpec2
  private type LazySeq[+A] = Stream[A]
  private type Projs       = Map[Path, Projection]

  final case class External(slices: NeedSlices, size: TableSize) extends TableData {
  }
  final case class Internal(slice: Slice) extends TableData {
    def slices = singleStreamT(slice)
    def size   = ExactSize(slice.size)
  }

  class BinaryTableData(left: TableData, right: TableData) {
    def cogroup(left: PairOf[F1], right: PairOf[F1], both: F2): T = ???
    def concat(): T                                               = ???
    def cross(spec: F2): T                                        = ???
    def zip(): T                                                  = ???
  }

  class Impl(val self: TableData) extends TableMethods[TableData] {
    private type LazySeqT[A] = StreamT[M, A]

    def asRep: TableRep[TableData] = ??? // TableRep[TableData](self, x => new Impl(x), companion)
    def companion = TableData

    def size: TableSize         = self.size
    def slices: LazySeqT[Slice] = self.slices
    def schemas: M[Set[JType]]  = ???

    def cogroup(leftKey: F1, rightKey: F1, that: T)(left: F1, right: F1, both: F2): T = ???
    def concat(that: T): T                                                            = ???
    def cross(that: T)(spec: F2): T                                                   = ???
    def zip(that: T): M[T]                                                            = ???

    def canonicalize(minLength: Int, maxLength: Int): T     = ???
    def compact(spec: F1, definedness: Definedness): T      = ???
    def distinct(key: F1): T                                = ???
    def mapWithSameSize(f: EndoA[LazySeqT[Slice]]): T       = ???
    def partitionMerge(partitionBy: F1)(f: T => M[T]): M[T] = ???
    def reduce[A: Monoid](reducer: CReducer[A]): M[A]       = ???
    def sample(size: Int, specs: Seq[F1]): M[TS]            = ???
    def sort(key: F1, order: DesiredSortOrder): M[T]        = ???
    def takeRange(start: Long, length: Long): T             = ???
    def transform(spec: F1): T                              = ???

    def force(): M[T]          = ???
    def load(tpe: JType): M[T] = ???
    def normalize(): T         = ???
    def paged(limit: Int): T   = ???

    def toArray[A: CValueType] : T    = ???
    def toJson: M[LazySeq[V]]         = ???
    def projections: Projs            = ???
    def withProjections(ps: Projs): T = ???
  }
}
