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
import scalaz.{ Source => _, _ }
import Scalaz.{ ToIdOps => _, _ }
import JDBM.{ SortedSlice, IndexMap }

private object addGlobalIdScanner extends Scanner {
  type A = Long
  val init = 0l
  def scan(a: Long, cols: ColumnMap, range: Range): A -> ColumnMap = {
    val globalIdColumn = new RangeColumn(range) with LongColumn { def apply(row: Int) = a + row }
    (a + range.end + 1, cols + (ColumnRef(CPath(CPathIndex(1)), CLong) -> globalIdColumn))
  }
}

class TableSelector[A <: Table](val table: A) {
  def >>(): Unit = table.toVector foreach println

  def filter(p: TransSpec[Source.type]) = table transform (root filter p)
}
object Table extends TableModule {
  implicit val codec = DataCodec.Precise

  def apply(json: String): BaseTable = fromJson(JParser.parseManyFromString(json).fold(throw _, x => x))
  def apply(file: jFile): BaseTable  = apply(file.slurpString)

  def toJson(dataset: Table): Need[Stream[JValue]] = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]         = toJson(table).copoint
  def fromData(data: Vector[Data]): BaseTable      = fromJson(data map dataToJValue)
  def fromFile(file: jFile): BaseTable             = fromJson((JParser parseManyFromFile file).orThrow)
  def fromString(json: String): BaseTable          = fromJson(Seq(JParser parseUnsafe json))
  def fromJValues(json: JValue*): BaseTable        = fromJson(json.toVector)
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

object TableCompanion {
  def apply[A <: Table](implicit z: TableCompanion[A]): TableCompanion[A] = z
}

trait TableCompanion[T <: ygg.table.Table] {
  type M[X]  = T#M[X]
  type Table = T

  lazy val sortMergeEngine = new MergeEngine

  def addGlobalId(spec: TransSpec1): TransSpec1                                        = Scan(WrapArray(spec), addGlobalIdScanner)
  def align(sourceL: T, alignL: TransSpec1, sourceR: T, alignR: TransSpec1): PairOf[T] = AlignTable[T](sourceL, alignL, sourceR, alignR)(this)

  def loadTable(mergeEngine: MergeEngine, indices: IndexMap, sortOrder: DesiredSortOrder): T = {
    import mergeEngine._
    val totalCount = indices.toList.map { case (_, sliceIndex) => sliceIndex.count }.sum

    // Map the distinct indices into SortProjections/Cells, then merge them
    def cellsMs: Stream[Need[Option[CellState]]] = indices.values.toStream.zipWithIndex map {
      case (SortedSlice(name, kslice, vslice, _, _, _, _), index) =>
        val slice = Slice(kslice.size, kslice.wrap(CPathIndex(0)).columns ++ vslice.wrap(CPathIndex(1)).columns)
        // We can actually get the last key, but is that necessary?
        Need(Some(CellState(index, new Array[Byte](0), slice, (k: Bytes) => Need(None))))

      case (JDBM.SliceIndex(name, dbFile, _, _, _, keyColumns, valColumns, count), index) =>
        // Elided untested code.
        ???
    }

    val head = StreamT.Skip(
      StreamT.wrapEffect(
        for (cellOptions <- cellsMs.sequence) yield {
          mergeProjections(sortOrder, cellOptions.flatMap(a => a)) { slice =>
            // only need to compare on the group keys (0th element of resulting table) between projections
            slice.columns.keys collect { case ColumnRef(path @ CPath(CPathIndex(0), _ @_ *), _) => path }
          }
        }
      )
    )

    val tab = apply(StreamT(Need(head)), ExactSize(totalCount))
    fixTable(tab transform TransSpec1.DerefArray1)
  }

  /**
    * Sorts the KV table by ascending or descending order based on a seq of transformations
    * applied to the rows.
    *
    * @see quasar.ygg.TableModule#groupByN(TransSpec1, DesiredSortOrder, Boolean)
    */
  private def groupExternalByN(table: T, groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Seq[T]] = {
    WriteTable[T](this).writeSorted(table, groupKeys, valueSpec, sortOrder, unique) map {
      case (streamIds, indices) =>
        val streams = indices.groupBy(_._1.streamId)
        streamIds.toStream map { streamId =>
          streams get streamId map (loadTable(sortMergeEngine, _, sortOrder)) getOrElse empty
        }
    }
  }

  /**
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param key The transspec to use to obtain the values to sort on
    * @param order Whether to sort ascending or descending
    */
  def sort[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder): F[T]       = sortCommon[F](table, key, order, unique = false)
  def sortUnique[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder): F[T] = sortCommon[F](table, key, order, unique = true)

  private def sortCommon[F[_]: Monad](table: T, key: TransSpec1, order: DesiredSortOrder, unique: Boolean): F[T] =
    groupByN[F](externalize(table), Seq(key), root, order, unique) map (_.headOption getOrElse empty)

  /**
    * Sorts the KV table by ascending or descending order based on a seq of transformations
    * applied to the rows.
    *
    * @param keys The transspecs to use to obtain the values to sort on
    * @param values The transspec to use to obtain the non-sorting values
    * @param order Whether to sort ascending or descending
    * @param unique If true, the same key values will sort into a single row, otherwise
    * we assign a unique row ID as part of the key so that multiple equal values are
    * preserved
    */
  def groupByN[F[_]: Monad](table: T, keys: Seq[TransSpec1], values: TransSpec1, order: DesiredSortOrder, unique: Boolean): F[Seq[T]] =
    ().point[F] map (_ => fixTables(groupExternalByN(externalize(table), keys, values, order, unique).value))

  def writeAlignedSlices(kslice: Slice, vslice: Slice, jdbmState: JDBMState, indexNamePrefix: String, sortOrder: DesiredSortOrder) =
    WriteTable[T](this).writeAlignedSlices(kslice, vslice, jdbmState, indexNamePrefix, sortOrder)

  /**
    * Passes over all slices and returns a new slices that is the concatenation
    * of all the slices. At some point this should lazily chunk the slices into
    * fixed sizes so that we can individually sort/merge.
    */
  def reduceSlices(slices: NeedSlices): NeedSlices = {
    def rec(ss: List[Slice], slices: NeedSlices): NeedSlices = {
      StreamT[Need, Slice](slices.uncons map {
        case Some((head, tail)) => StreamT.Skip(rec(head :: ss, tail))
        case None if ss.isEmpty => StreamT.Done
        case None               => StreamT.Yield(Slice.concat(ss.reverse), emptyStreamT())
      })
    }

    rec(Nil, slices)
  }

  def load(table: Table, tpe: JType, projections: Map[Path, Projection]): M[Table] = {
    val reduced = table reduce new CReducer[Set[Path]] {
      def reduce(schema: CSchema, range: Range): Set[Path] = schema columns JTextT flatMap {
        case s: StrColumn => range collect { case i if s isDefinedAt i => Path(s(i)) }
        case _            => Set()
      }
    }
    reduced map { paths =>
      val projs = paths.toList flatMap projections.get
      apply(
        projs foldMap (_ getBlockStreamForType tpe),
        ExactSize(projs.foldMap(_.length)(Monoid[Long]))
      )
    }
  }

  def constSliceTable[A: CValueType](vs: Array[A], mkColumn: Array[A] => Column): Table = apply(
    singleStreamT(Slice(vs.length, columnMap(ColumnRef.id(CValueType[A]) -> mkColumn(vs)))),
    ExactSize(vs.length)
  )
  def constSingletonTable(singleType: CType, column: Column): Table = apply(
    singleStreamT(Slice(1, columnMap(ColumnRef.id(singleType) -> column))),
    ExactSize(1)
  )

  def constBoolean(v: Set[Boolean]): Table    = constSliceTable[Boolean](v.toArray, ArrayBoolColumn(_))
  def constLong(v: Set[Long]): Table          = constSliceTable[Long](v.toArray, ArrayLongColumn(_))
  def constDouble(v: Set[Double]): Table      = constSliceTable[Double](v.toArray, ArrayDoubleColumn(_))
  def constDecimal(v: Set[BigDecimal]): Table = constSliceTable[BigDecimal](v.toArray, ArrayNumColumn(_))
  def constString(v: Set[String]): Table      = constSliceTable[String](v.toArray, ArrayStrColumn(_))
  def constDate(v: Set[DateTime]): Table      = constSliceTable[DateTime](v.toArray, ArrayDateColumn(_))
  def constNull: Table                        = constSingletonTable(CNull, new InfiniteColumn with NullColumn)
  def constEmptyObject: Table                 = constSingletonTable(CEmptyObject, new InfiniteColumn with EmptyObjectColumn)
  def constEmptyArray: Table                  = constSingletonTable(CEmptyArray, new InfiniteColumn with EmptyArrayColumn)

  def empty: T
  def apply(slices: NeedSlices, size: TableSize): T

  def newInternalTable(slice: Slice): T
  def newExternalTable(slices: NeedSlices, size: TableSize): T

  def cogroup(self: Table, leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table =
    CogroupTable[T](self, leftKey, rightKey, that)(leftResultTrans, rightResultTrans, bothResultTrans)(this)

  def externalize(table: T): T = newExternalTable(table.slices, table.size)
}


sealed trait TableAdt {
  def size: TableSize
  def slices: NeedSlices
}
object TableAdt {
  final case class External(slices: NeedSlices, size: TableSize) extends TableAdt
  final case class Internal(slice: Slice) extends TableAdt {
    def slices = singleStreamT(slice)
    def size   = ExactSize(slice.size)
  }

  class Impl(val table: TableAdt) extends TableMethods {
    type Table = TableAdt

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

trait Table extends TableMethods {
  type Table <: ygg.table.Table

  def companion: TableCompanion[Table]
}

object TableMethods {
  type Aux[T] = TableMethods { type Table = T }
}

trait HasTableMethods[R, T] {
  def methods(x: R): TableMethods { type Table = T }

  // def methods(x: Table { type Table = T }): TableMethods { type Table = T }
}
object HasTableMethods {
  implicit def oldTableMethods[T <: ygg.table.Table] = new HasTableMethods[T { type Table = T }, T] {
    def methods(x: T { type Table = T }) = x
  }
  implicit def tableAdtMethods: HasTableMethods[TableAdt, TableAdt] = new HasTableMethods[TableAdt, TableAdt] {
    def methods(x: TableAdt) = new TableAdt.Impl(x)
  }
}

trait TableMethods {
  type Table
  type M[X]       = Need[X]
  type NeedSlices = StreamT[M, Slice]

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

  def canonicalize(length: Int): Table
  def canonicalize(minLength: Int, maxLength: Int): Table
  def columns: ColumnMap
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
  def toData: Data
  def toJValues: Stream[JValue]
  def toJson: M[Stream[JValue]]
  def zip(t2: Table): M[Table]

  def slicesStream: Stream[Slice]
  def toVector: Vector[JValue]
  def toDataStream: Stream[Data]
}
