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

  implicit def tableMethods(table: Table): TableMethods[Table]
  def empty: Table
  def fromSlices(slices: NeedSlices, size: TableSize): Table

  def load(table: Table, tpe: JType): Need[Table] = {
    val reduced = table reduce new CReducer[Set[Path]] {
      def reduce(schema: CSchema, range: Range): Set[Path] = schema columns JTextT flatMap {
        case s: StrColumn => range collect { case i if s isDefinedAt i => Path(s(i)) }
        case _            => Set()
      }
    }
    reduced map { paths =>
      val projs = paths.toList flatMap (table.projections get _)
      apply(
        projs foldMap (_ getBlockStreamForType tpe),
        ExactSize(projs.foldMap(_.length)(Monoid[Long]))
      )
    }
  }

  def apply(file: jFile): Table                         = apply(file.slurpString)
  def apply(slices: NeedSlices, size: TableSize): Table = fromSlices(slices, size)
  def apply(json: String): Table                        = fromJValues(JParser.parseManyFromString(json).fold[Seq[JValue]](throw _, x => x))

  def fromData(data: Vector[Data]): Table          = fromJValues(data map dataToJValue)
  def fromFile(file: jFile): Table                 = fromJValues((JParser parseManyFromFile file).orThrow)
  def fromString(json: String): Table              = fromJValues(Seq(JParser parseUnsafe json))
  def toJson(dataset: Table): Need[Stream[JValue]] = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]         = toJson(table).copoint

  def constBoolean(v: Set[Boolean]): Table    = constSliceTable[Boolean](v.toArray, ArrayBoolColumn(_))
  def constLong(v: Set[Long]): Table          = constSliceTable[Long](v.toArray, ArrayLongColumn(_))
  def constDouble(v: Set[Double]): Table      = constSliceTable[Double](v.toArray, ArrayDoubleColumn(_))
  def constDecimal(v: Set[BigDecimal]): Table = constSliceTable[BigDecimal](v.toArray, ArrayNumColumn(_))
  def constString(v: Set[String]): Table      = constSliceTable[String](v.toArray, ArrayStrColumn(_))
  def constDate(v: Set[DateTime]): Table      = constSliceTable[DateTime](v.toArray, ArrayDateColumn(_))
  def constNull: Table                        = constSingletonTable(CNull, new InfiniteColumn with NullColumn)
  def constEmptyObject: Table                 = constSingletonTable(CEmptyObject, new InfiniteColumn with EmptyObjectColumn)
  def constEmptyArray: Table                  = constSingletonTable(CEmptyArray, new InfiniteColumn with EmptyArrayColumn)
  def constSliceTable[A: CValueType](vs: Array[A], mkColumn: Array[A] => Column): Table = fromSlices(
    singleStreamT(Slice(vs.length, columnMap(ColumnRef.id(CValueType[A]) -> mkColumn(vs)))),
    ExactSize(vs.length)
  )
  def constSingletonTable(singleType: CType, column: Column): Table = fromSlices(
    singleStreamT(Slice(1, columnMap(ColumnRef.id(singleType) -> column))),
    ExactSize(1)
  )

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

  def slices: NeedSlices
  def size: TableSize
  def projections: Map[Path, Projection]

  def self: Table
  def asRep: TableRep[Table]
  def companion: TableMethodsCompanion[Table]

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
  def takeRange(startIndex: Long, numberToTake: Long): Table
  def toArray[A](implicit tpe: CValueType[A]): Table
  def toJson: M[Stream[JValue]]
  def zip(t2: Table): M[Table]

  def withProjections(ps: Map[Path, Projection]): Table

  def canonicalize(length: Int): Table = canonicalize(length, length)
  def slicesStream: Stream[Slice]  = slices.toStream.value

  def toJsonString: String      = toJValues mkString "\n"
  def toVector: Vector[JValue]  = toJValues.toVector
  def toJValues: Stream[JValue] = slicesStream flatMap (_.toJsonElements)
  def columns: ColumnMap        = slicesStream.head.columns
  def fields: Vector[JValue]    = toVector
  def dump(): Unit              = toVector foreach println
}
