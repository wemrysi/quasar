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

import ygg._, common._, json._, trans._, data._
import quasar._
import scalaz._, Scalaz._

trait TableConfig {
  def maxSliceSize: Int = 10
  // This is a slice size that we'd like our slices to be at least as large as.
  def minIdealSliceSize: Int = maxSliceSize / 4
  // This is what we consider a "small" slice. This may affect points where
  // we take proactive measures to prevent problems caused by small slices.
  def smallSliceSize: Int = 3
  def maxSaneCrossSize: Long = 2400000000L // 2.4 billion
}

trait TableCompanion[Table] extends TableConfig {
  self =>

  lazy val sortMergeEngine = new MergeEngine

  private implicit def thisRep = TableRep make self

  def sizeOf(table: Table): TableSize
  def slicesOf(table: Table): StreamT[Need, Slice]
  def projectionsOf(table: Table): Map[Path, Projection]
  def withProjections(table: Table, ps: ProjMap): Table

  def empty: Table
  def fromSlices(slices: NeedSlices, size: TableSize): Table

  private lazy val addGlobalIdScanner = Scanner(0L) { (a, cols, range) =>
    val globalIdColumn = new RangeColumn(range) with LongColumn { def apply(row: Int) = a + row }
    (a + range.end + 1, cols + (ColumnRef(CPath(CPathIndex(1)), CLong) -> globalIdColumn))
  }

  /**
    * Return the subtable where each group key in keyIds is set to
    * the corresponding value in keyValues.
    */
  def getSubTable(tableIndex: TableIndex, keyIds: Seq[Int], keyValues: Seq[RValue]): Table = {
    import tableIndex.indices

    // Each slice index will build us a slice, so we just return a
    // table of those slices.
    //
    // Currently we assemble the slices eagerly. After some testing
    // it might be the case that we want to use StreamT in a more
    // traditional (lazy) manner.
    var size = 0L
    val slices: List[Slice] = indices.map { sliceIndex =>
      val rows  = getRowsForKeys(sliceIndex, keyIds, keyValues)
      val slice = buildSubSlice(sliceIndex, rows)
      size += slice.size
      slice
    }

    lazyTable(slices, ExactSize(size))
  }

  /**
    * Return the subtable where each group key in keyIds is set to
    * the corresponding value in keyValues.
    */
  def getSubTable(sliceIndex: SliceIndex, keyIds: Seq[Int], keyValues: Seq[RValue]) =
    buildSubTable(sliceIndex, getRowsForKeys(sliceIndex, keyIds, keyValues))

  /**
    * Given a set of rows, builds the appropriate subslice.
    */
  private[table] def buildSubTable(sliceIndex: SliceIndex, rows: ArrayIntList): Table =
    fromSlices(singleStreamT(buildSubSlice(sliceIndex, rows)), ExactSize(rows.size))

  /**
    * Given a set of rows, builds the appropriate slice.
    */
  private[table] def buildSubSlice(sliceIndex: SliceIndex, rows: ArrayIntList): Slice =
    if (rows.isEmpty)
      Slice.empty
    else
      sliceIndex.valueSlice.remap(rows)

  def fromRValues(values: Stream[RValue]): Table = fromRValues(values, maxSliceSize)
  def fromRValues(values: Stream[RValue], sliceSize: Int): Table = {
    def makeSlice(data: Stream[RValue]): Slice -> Stream[RValue] =
      data splitAt sliceSize leftMap (Slice fromRValues _)

    fromSlices(
      unfoldStream(values)(events => Need(events.nonEmpty option makeSlice(events.toStream))),
      ExactSize(values.length)
    )
  }

  /**
    * Returns the rows specified by the given group key values.
    */
  private def getRowsForKeys(sliceIndex: SliceIndex, keyIds: Seq[Int], keyValues: Seq[RValue]): ArrayIntList = {
    var rows: ArrayIntList = sliceIndex.dict.getOrElse((keyIds(0), keyValues(0)), ArrayIntList.empty)
    var i: Int             = 1
    while (i < keyIds.length && !rows.isEmpty) {
      rows = rows intersect sliceIndex.dict.getOrElse((keyIds(i), keyValues(i)), ArrayIntList.empty)
      i += 1
    }
    rows
  }
  /**
    * For a list of slice indices, return a slice containing all the
    * rows for which any of the indices matches.
    *
    * NOTE: Only the first slice's value spec is used to construct
    * the slice since it's assumed that all slices have the same
    * value spec.
    */
  def joinSubSlices(tpls: List[(SliceIndex, (Seq[Int], Seq[RValue]))]): Slice = tpls match {
    case Nil =>
      abort("empty slice") // FIXME
    case (index, (ids, vals)) :: tail =>
      var rows = getRowsForKeys(index, ids, vals)
      tail.foreach {
        case (index, (ids, vals)) =>
          rows = rows union getRowsForKeys(index, ids, vals)
      }
      buildSubSlice(index, rows)
  }

  def addGlobalId(spec: TransSpec1): TransSpec1 = Scan(WrapArray(spec), addGlobalIdScanner)

  def writeAlignedSlices(kslice: Slice, vslice: Slice, jdbmState: JDBMState, indexNamePrefix: String, sortOrder: DesiredSortOrder) =
    WriteTable[Table].writeAlignedSlices(kslice, vslice, jdbmState, indexNamePrefix, sortOrder)

  import JDBM.{ IndexMap, SortedSlice }
  def loadTable(mergeEngine: MergeEngine, indices: IndexMap, sortOrder: DesiredSortOrder): Table = {
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

    fromSlices(StreamT(Need(head)), ExactSize(totalCount)) transform (`.` \ 1)
  }

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
  def groupByN(table: Table, keys: Seq[TransSpec1], values: TransSpec1, order: DesiredSortOrder, unique: Boolean): Seq[Table] =
    WriteTable[Table].groupByN(externalize(table), keys, values, order, unique)

  def load(table: Table, tpe: JType): Need[Table] = {
    val reduced = table reduce new CReducer[Set[Path]] {
      def reduce(schema: CSchema, range: Range): Set[Path] = schema columns JTextT flatMap {
        case s: StrColumn => range collect { case i if s isDefinedAt i => Path(s(i)) }
        case _            => Set()
      }
    }
    reduced map { paths =>
      val projs = paths.toList flatMap (projectionsOf(table) get _)
      apply(
        projs foldMap (_ getBlockStreamForType tpe),
        ExactSize(projs.foldMap(_.length)(Monoid[Long]))
      )
    }
  }

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


  def apply(file: jFile): Table                         = apply(file.slurpString)
  def apply(slices: NeedSlices, size: TableSize): Table = fromSlices(slices, size)
  def apply(json: String): Table                        = fromJValues(JParser.parseManyFromString(json).fold[Seq[JValue]](throw _, x => x))

  def fromData(data: Vector[Data]): Table = fromJValues(data map dataToJValue)
  def fromFile(file: jFile): Table        = fromJValues((JParser parseManyFromFile file).orThrow)
  def fromString(json: String): Table     = fromJValues(Seq(JParser parseUnsafe json))

  def externalize(table: Table): Table = fromSlices(slicesOf(table), sizeOf(table))

  def merge(grouping: GroupingSpec[Table])(body: (RValue, GroupId => Need[Table]) => Need[Table]): Need[Table] =
    MergeTable[Table](grouping)(body)

  def constBoolean(v: Boolean*): Table    = constSliceTable[Boolean](v.toArray, ArrayBoolColumn(_))
  def constLong(v: Long*): Table          = constSliceTable[Long](v.toArray, ArrayLongColumn(_))
  def constDouble(v: Double*): Table      = constSliceTable[Double](v.toArray, ArrayDoubleColumn(_))
  def constDecimal(v: BigDecimal*): Table = constSliceTable[BigDecimal](v.toArray, ArrayNumColumn(_))
  def constString(v: String*): Table      = constSliceTable[String](v.toArray, ArrayStrColumn(_))
  def constDate(v: DateTime*): Table      = constSliceTable[DateTime](v.toArray, ArrayDateColumn(_))

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

  def fromJValues(values: Seq[JValue]): Table = fromJValues(values, maxSliceSize)
  def fromJValues(values: Seq[JValue], sliceSize: Int): Table = {
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
