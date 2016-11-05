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
import quasar._
import scalaz.{ Source => _, _ }
import Scalaz.{ ToIdOps => _, _ }

trait TableMethodsCompanion[Table] {
  implicit lazy val codec = DataCodec.Precise

  def empty: Table
  def fromSlices(slices: NeedSlices, size: TableSize): Table

  def apply(file: jFile): Table                         = apply(file.slurpString)
  def apply(slices: NeedSlices, size: TableSize): Table = fromSlices(slices, size)
  def apply(json: String): Table                        = fromJValues(JParser.parseManyFromString(json).fold[Seq[JValue]](throw _, x => x))

  def fromJValues(values: Seq[JValue]): Table = fromJValues(values, None)
  def fromJValues(values: Seq[JValue], maxSliceSize: Option[Int]): Table = {
    val sliceSize = maxSliceSize getOrElse yggConfig.maxSliceSize
    def makeSlice(data: Stream[JValue]): Slice -> Stream[JValue] = {
      @tailrec def buildColArrays(from: Stream[JValue], into: ArrayColumnMap, sliceIndex: Int): ArrayColumnMap -> Int = from match {
        case jv #:: xs => buildColArrays(xs, Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize), sliceIndex + 1)
        case _         => (into, sliceIndex)
      }
      val (prefix, suffix) = data splitAt sliceSize
      val (refs, size)     = buildColArrays(prefix.toStream, Map(), 0)
      val slice            = Slice(size, refs)

      slice -> suffix
    }

    fromSlices(
      unfoldStream(values.toStream)(evts => Need(evts.nonEmpty option makeSlice(evts))),
      ExactSize(values.length)
    )
  }
}

trait TableMethods[Table] {
  type M[+X] = Need[X]

  def self: Table
  def companion: TableMethodsCompanion[Table]

  /**
    * Return an indication of table size, if known
    */
  def size: TableSize

  /**
    * Folds over the table to produce a single value (stored in a singleton table).
    */
  def reduce[A: Monoid](reducer: CReducer[A]): M[A]

  /**
    * Removes all rows in the table for which definedness is satisfied
    * Remaps the indicies.
    */
  def compact(spec: TransSpec1, definedness: Definedness): Table
  def compact(spec: TransSpec1): Table = compact(spec, AnyDefined)

  /**
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param key The transspec to use to obtain the values to sort on
    * @param order Whether to sort ascending or descending
    */
  def sort(key: TransSpec1, order: DesiredSortOrder): M[Table]

  /**
    * Performs a one-pass transformation of the keys and values in the table.
    * If the key transform is not identity, the resulting table will have
    * unknown sort order.
    */
  def transform(spec: TransSpec1): Table

  /**
    * Cogroups this table with another table, using equality on the specified
    * transformation on rows of the table.
    */
  def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(left: TransSpec1, right: TransSpec1, both: TransSpec2): Table

  /**
    * Performs a full cartesian cross on this table with the specified table,
    * applying the specified transformation to merge the two tables into
    * a single table.
    */
  def cross(that: Table)(spec: TransSpec2): Table

  /**
    * Force the table to a backing store, and provice a restartable table
    * over the results.
    */
  def force: M[Table]

  /**
    * For each distinct path in the table, load all columns identified by the specified
    * jtype and concatenate the resulting slices into a new table.
    */
  def load(tpe: JType): M[Table]

  def canonicalize(minLength: Int, maxLength: Int): Table
  def concat(t2: Table): Table
  def distinct(key: TransSpec1): Table
  def mapWithSameSize(f: EndoA[NeedSlices]): Table
  def normalize: Table
  def paged(limit: Int): Table
  def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table]
  def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]]
  def schemas: M[Set[JType]]
  def slices: NeedSlices
  def takeRange(startIndex: Long, numberToTake: Long): Table
  def toArray[A](implicit tpe: CValueType[A]): Table
  def toJson: M[Stream[JValue]]
  def zip(t2: Table): M[Table]

  def projections: Map[Path, Projection]
  def withProjections(ps: Map[Path, Projection]): Table

  def canonicalize(length: Int): Table = canonicalize(length, length)
}
