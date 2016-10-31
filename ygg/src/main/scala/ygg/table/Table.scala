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
import quasar.ejson.EJson
import scalaz.{ Source => _, _ }, Scalaz._

class TableSelector[A <: Table](val table: A { type Table = A }) {
  def >>(): Unit = table.toVector foreach println

  def filter(p: TransSpec[Source.type]) = table transform (root filter p)

  // def innerCross(that: A, left: String, right: String): A = {
  //   val spec: TransSpec2 = InnerObjectConcat(WrapObject(Leaf(SourceLeft), left), WrapObject(Leaf(SourceRight), right))

  //   table.companion.cross(table, that, None)(spec).value._2
  // }
}
object Table extends BlockTableBase

trait TableConstructors[T <: ygg.table.Table] {
  type JsonRep
  type Seq[A]

  def maxSliceSize: Int

  def empty: T
  def fromData(data: Data): T
  def fromEJson[A](json: EJson[A])(f: A => JsonRep): T
  def fromFile(file: jFile): T
  def fromJson(json: String): T
  def fromJValues(data: Seq[JsonRep]): T
  def fromRValues(data: Seq[RValue]): T
  def fromSlice(data: Slice): T
  def fromSlices(data: Seq[Slice]): T
}

// trait TableRep[T] {
//   type Table = T with ygg.table.Table
//   type Companion <: TableCompanion[Table]

//   def companion: Companion
// }

object TableCompanion {
  def apply[A <: Table](implicit z: TableCompanion[A]): TableCompanion[A] = z
}

trait TableCompanion[T <: ygg.table.Table] {
  type Table = T
  type NeedTable = Need[Table]

  def apply(slices: NeedSlices, size: TableSize): T

  def empty: T
  def constString(v: scSet[String]): T
  def constLong(v: scSet[Long]): T
  def constDouble(v: scSet[Double]): T
  def constDecimal(v: scSet[BigDecimal]): T
  def constDate(v: scSet[DateTime]): T
  def constBoolean(v: scSet[Boolean]): T
  def constNull: T
  def constEmptyObject: T
  def constEmptyArray: T
  def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): T

  def merge(grouping: GroupingSpec)(body: (RValue, GroupId => Need[T]) => Need[T]): Need[T]
  def align(sourceL: T, alignL: TransSpec1, sourceR: T, alignR: TransSpec1): PairOf[Table]
  // def cross(left: T, right: T, orderHint: Option[CrossOrder])(spec: TransSpec2): Need[CrossOrder -> Table]

  def cogroup(self: Table, leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table =
    Cogrouped[T](self, leftKey, rightKey, that)(leftResultTrans, rightResultTrans, bothResultTrans)(this)

  def sort[F[_]: Monad](table: T)(key: TransSpec1, order: DesiredSortOrder): F[T] = table match {
    case _: SingletonTable => table.point[F]
    case _: InternalTable  => sort[F](table.toExternalTable.asInstanceOf[T])(key, order)
    case _: ExternalTable  => groupByN[F](table, Seq(key), root, order) map (_.headOption getOrElse empty)
  }
  def groupByN[F[_]: Monad](table: T, keys: Seq[TransSpec1], values: TransSpec1, order: DesiredSortOrder): F[Seq[T]] = {
    ().point[F] map (_ =>
      table.groupByN(keys, values, order, unique = false).value.toVector.map(x => x.asInstanceOf[T])
    )
  }

  def constSingletonTable(singleType: CType, column: Column): T
  def constSliceTable[A: CValueType](vs: Array[A], mkColumn: Array[A] => Column): T
  def fromJson(data: Seq[JValue]): T
  def load(table: T, tpe: JType): Need[T]
  def singleton(slice: Slice): T
}

trait TemporaryTableStrutCompanion {
  type Table
  def align(sourceL: Table, alignL: TransSpec1, sourceR: Table, alignR: TransSpec1): PairOf[Table] = ???
}
trait TemporaryTableStrut extends Table {
  /** XXX FIXME */
  // def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable                                                               = ???
  def groupByN(groupKeys: scSeq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[scSeq[Table]] = ???
  def toInternalTable(limit: Int): ExternalTable \/ InternalTable                                                                     = ???
  def toExternalTable(): ExternalTable                                                                                                = ???

  def takeRange(startIndex: Long, numberToTake: Long): Table = takeRangeDefaultImpl(startIndex, numberToTake)
  def takeRangeDefaultImpl(startIndex: Long, numberToTake: Long): Table
}

trait InternalTable extends Table {
  def slice: Slice
}
trait ExternalTable extends Table {
}
trait SingletonTable extends Table {
}
trait Table {
  type NeedTable = Need[Table]

  type Table <: ygg.table.Table
  type InternalTable <: Table with ygg.table.InternalTable
  type ExternalTable <: Table with ygg.table.ExternalTable
  type SingletonTable <: Table with ygg.table.SingletonTable

  /**
    * Return an indication of table size, if known
    */
  def size: TableSize

  /**
    * Folds over the table to produce a single value (stored in a singleton table).
    */
  def reduce[A: Monoid](reducer: CReducer[A]): Need[A]

  /**
    * Removes all rows in the table for which definedness is satisfied
    * Remaps the indicies.
    */
  def compact(spec: TransSpec1, definedness: Definedness): Table

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
  def force: NeedTable

  /**
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param sortKey The transspec to use to obtain the values to sort on
    * @param sortOrder Whether to sort ascending or descending
    */
  // def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable

  /**
    * Sorts the KV table by ascending or descending order based on a seq of transformations
    * applied to the rows.
    *
    * @param groupKeys The transspecs to use to obtain the values to sort on
    * @param valueSpec The transspec to use to obtain the non-sorting values
    * @param sortOrder Whether to sort ascending or descending
    * @param unique If true, the same key values will sort into a single row, otherwise
    * we assign a unique row ID as part of the key so that multiple equal values are
    * preserved
    */
  def groupByN(groupKeys: scSeq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[scSeq[Table]]

  /**
    * Converts a table to an internal table, if possible. If the table is
    * already an `InternalTable` or a `SingletonTable`, then the conversion
    * will always succeed. If the table is an `ExternalTable`, then if it has
    * less than `limit` rows, it will be converted to an `InternalTable`,
    * otherwise it will stay an `ExternalTable`.
    */
  def toInternalTable(limit: Int): ExternalTable \/ InternalTable
  def toExternalTable(): ExternalTable

  /**
    * For each distinct path in the table, load all columns identified by the specified
    * jtype and concatenate the resulting slices into a new table.
    */
  def load(tpe: JType): NeedTable

  def canonicalize(length: Int): Table
  def canonicalize(minLength: Int, maxLength: Int): Table
  def columns: ColumnMap
  def companion: TableCompanion[Table]
  def concat(t2: Table): Table
  def distinct(key: TransSpec1): Table
  def mapWithSameSize(f: NeedSlices => NeedSlices): Table
  def normalize: Table
  def paged(limit: Int): Table
  def partitionMerge(partitionBy: TransSpec1)(f: Table => NeedTable): NeedTable
  def sample(sampleSize: Int, specs: Seq[TransSpec1]): Need[Seq[Table]]
  def schemas: Need[Set[JType]]
  def slices: NeedSlices
  def takeRange(startIndex: Long, numberToTake: Long): Table
  def toArray[A](implicit tpe: CValueType[A]): Table
  def toData: Data
  def toJValues: Stream[JValue]
  def toJson: Need[Stream[JValue]]
  def zip(t2: Table): NeedTable

  def slicesStream: Stream[Slice]
  def toVector: Vector[JValue]
  def toDataStream: Stream[Data]
  def fields = slicesStream.flatMap(_.toJsonElements).toVector
}
