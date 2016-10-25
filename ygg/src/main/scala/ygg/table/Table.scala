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

import scalaz._
import ygg._, common._, json._
import trans._
import quasar._
import quasar.ejson.EJson

object Table {
  implicit val codec = DataCodec.Precise

  def fromEJson[A](json: EJson[A])(f: A => JValue): Table = ???

  def dataToJValue(d: Data): JValue = d match {
    case Data.NA => JUndefined
    case _       =>
      DataCodec render d match {
        case -\/(e)  => throw new RuntimeException(e.toString)
        case \/-(jv) =>
          (JParser parseFromString jv).disjunction match {
            case -\/(t) => throw t
            case \/-(r) => r
          }
      }
  }

  def fromData(data: Vector[Data]): Table = {
    val res = fromJson(data map dataToJValue)
    println("DATA: " + data)
    println("TABLE: " + res.toJValues.mkString("\n"))
    res
  }
  def fromFile(file: jFile): Table        = fromJson((JParser parseManyFromFile file).orThrow)
  def fromString(json: String): Table     = fromJson(Seq(JParser parseUnsafe json))
  def fromJValues(json: JValue*): Table   = fromJson(json.toVector)
  def fromJson(json: Seq[JValue]): Table  = BlockTable fromJson json
}

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

trait TableRep[T] {
  type Table = T with ygg.table.Table
  type Companion <: TableCompanion[Table]

  def companion: Companion
}

trait TableCompanion[T <: ygg.table.Table] {
  type Table = T
  type NeedTable = Need[Table]

  def apply(slices: NeedSlices, size: TableSize): Table

  def empty: Table
  def constString(v: scSet[String]): Table
  def constLong(v: scSet[Long]): Table
  def constDouble(v: scSet[Double]): Table
  def constDecimal(v: scSet[BigDecimal]): Table
  def constDate(v: scSet[DateTime]): Table
  def constBoolean(v: scSet[Boolean]): Table
  def constNull: Table
  def constEmptyObject: Table
  def constEmptyArray: Table
  def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): Table

  def merge(grouping: GroupingSpec)(body: (RValue, GroupId => NeedTable) => NeedTable): NeedTable
  def align(sourceL: Table, alignL: TransSpec1, sourceR: Table, alignR: TransSpec1): PairOf[Table]
  def join(left: Table, right: Table, orderHint: Option[JoinOrder])(lspec: TransSpec1, rspec: TransSpec1, joinSpec: TransSpec2): Need[JoinOrder -> Table]
  def cross(left: Table, right: Table, orderHint: Option[CrossOrder])(spec: TransSpec2): Need[CrossOrder -> Table]
}

trait Table {
  type NeedTable = Need[Table]
  type Table <: ygg.table.Table

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

  def canonicalize(length: Int): Table
  def companion: TableCompanion[Table]
  def concat(t2: Table): Table
  def distinct(spec: TransSpec1): Table
  def paged(limit: Int): Table
  def partitionMerge(partitionBy: TransSpec1)(f: Table => NeedTable): NeedTable
  def schemas: Need[Set[JType]]
  def slices: NeedSlices
  def sort(key: TransSpec1, order: DesiredSortOrder): NeedTable
  def takeRange(startIndex: Long, numberToTake: Long): Table
  def toArray[A](implicit tpe: CValueType[A]): Table
  def toData: Stream[Data]
  def toJValues: Stream[JValue]
  def toJson: Need[Stream[JValue]]
  def zip(t2: Table): NeedTable
}
