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
import quasar.Data

sealed trait TableData {
  def size: TableSize
  def slices: NeedSlices
}
object TableData {
  final case class External(slices: NeedSlices, size: TableSize) extends TableData
  final case class Internal(slice: Slice) extends TableData {
    def slices = singleStreamT(slice)
    def size   = ExactSize(slice.size)
  }

  class Impl(val table: TableData) extends TableMethods {
    type Table = TableData

    def canonicalize(length: Int): Table                                                                                              = ???
    def canonicalize(minLength: Int, maxLength: Int): Table                                                                           = ???
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table = ???
    def columns: ColumnMap                                                                                                            = ???
    def compact(spec: TransSpec1, definedness: Definedness): Table                                                                    = ???
    def concat(t2: Table): Table                                                                                                      = ???
    def cross(that: Table)(spec: TransSpec2): Table                                                                                   = ???
    def distinct(key: TransSpec1): Table                                                                                              = ???
    def force: M[Table]                                                                                                               = ???
    def load(tpe: JType): M[Table]                                                                                                    = ???
    def mapWithSameSize(f: EndoA[NeedSlices]): Table                                                                                  = ???
    def normalize: Table                                                                                                              = ???
    def paged(limit: Int): Table                                                                                                      = ???
    def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table]                                                       = ???
    def reduce[A: Monoid](reducer: CReducer[A]): M[A]                                                                                 = ???
    def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]]                                                                = ???
    def schemas: M[Set[JType]]                                                                                                        = ???
    def size: TableSize                                                                                                               = ???
    def slices: NeedSlices                                                                                                            = ???
    def slicesStream: Stream[Slice]                                                                                                   = ???
    def takeRange(startIndex: Long, numberToTake: Long): Table                                                                        = ???
    def toArray[A](implicit tpe: CValueType[A]): Table                                                                                = ???
    def toData: Data                                                                                                                  = ???
    def toDataStream: Stream[Data]                                                                                                    = ???
    def toJValues: Stream[JValue]                                                                                                     = ???
    def toJson: M[Stream[JValue]]                                                                                                     = ???
    def toVector: Vector[JValue]                                                                                                      = ???
    def transform(spec: TransSpec1): Table                                                                                            = ???
    def zip(t2: Table): M[Table]                                                                                                      = ???
  }
}
