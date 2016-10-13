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

import ygg._, common._, json._, data._
import scalaz._, Scalaz._

class PlayTable extends ColumnarTableModule {
  private val idGen       = new AtomicIntIdSource(new GroupId(_))
  def newGroupId: GroupId = idGen.nextId()

  object Table extends TableCompanion

  class Table(slices: NeedSlices, size: TableSize) extends ColumnarTable(slices, size) with NoLoadTable with NoSortTable with NoGroupTable {
    def slicesStream: Stream[Slice]  = slices.toStream.value
    def jvalueStream: Stream[JValue] = slicesStream flatMap (_.toJsonElements)
    def columns: ColumnMap           = slicesStream.head.columns
    def toVector: Vector[JValue]     = jvalueStream.toVector

    def companion = Table
    // Deadlock
    // override def toString = toJson.value.mkString("TABLE{ ", ", ", "}")

    private def colstr(kv: ColumnRef -> Column): String = {
      val (ColumnRef(path, tpe), column) = kv
      s"$path: $tpe => $column"
    }

    override def toString = jvalueStream mkString "\n"
  }
  trait TableCompanion extends ColumnarTableCompanion {
    def apply(slices: NeedSlices, size: TableSize): Table                                                  = new Table(slices, size)
    def singleton(slice: Slice): Table                                                                     = new Table(slice :: StreamT.empty[Need, Slice], ExactSize(1))
    def align(sourceL: Table, alignL: TransSpec1, sourceR: Table, alignR: TransSpec1): Need[PairOf[Table]] = ???
  }

  private def makeSlice(sampleData: Stream[JValue], sliceSize: Int): (Slice, Stream[JValue]) = {
    @tailrec def buildColArrays(from: Stream[JValue], into: ArrayColumnMap, sliceIndex: Int): (ArrayColumnMap, Int) = {
      from match {
        case jv #:: xs =>
          val refs = Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize)
          buildColArrays(xs, refs, sliceIndex + 1)
        case _ =>
          (into, sliceIndex)
      }
    }

    val (prefix, suffix) = sampleData.splitAt(sliceSize)
    val (refs, size)     = buildColArrays(prefix.toStream, Map(), 0)
    val slice            = Slice(size, refs)

    slice -> suffix
  }

  // production-path code uses fromRValues, but all the tests use fromJson
  // this will need to be changed when our tests support non-json such as CDate and CPeriod
  def fromJson0(values: Stream[JValue], sliceSize: Int): Table = Table(
    StreamT.unfoldM(values)(events => Need(events.nonEmpty option makeSlice(events.toStream, sliceSize))),
    ExactSize(values.length)
  )

  def fromJson(values: Seq[JValue], maxSliceSize: Option[Int]): Table =
    fromJson0(values.toStream, maxSliceSize getOrElse yggConfig.maxSliceSize)

  def toJson(dataset: Table): Need[Stream[JValue]]          = dataset.toJson.map(_.toStream)
  def toJsonSeq(table: Table): Seq[JValue]                  = toJson(table).copoint
  def fromJson(data: Seq[JValue]): Table                    = fromJson0(data.toStream, yggConfig.maxSliceSize)
}

object PlayTable extends PlayTable {
  def apply(json: String): Table = fromJson(JParser.parseManyFromString(json).fold(throw _, x => x))
  def apply(file: jFile): Table  = apply(file.slurpString)

  import matryoshka._
  import quasar._, sql._, SemanticAnalysis._, RenderTree.ops._

  def compile(query: String): String \/ Fix[LogicalPlan] =
    for {
      select <- fixParser.parse(Query(query)).leftMap(_.toString)
      attr   <- AllPhases(select).leftMap(_.toString)
      cld    <- Compiler.compile(attr).leftMap(_.toString)
    } yield cld

  def lp(q: String): LP = (
    compile(q) map Optimizer.optimize flatMap (q =>
      (LogicalPlan ensureCorrectTypes q).disjunction
        leftMap (_.list.toList mkString ";")
    )
  ).fold(abort, LP)

  def zips = PlayTable(new jFile("it/src/main/resources/tests/zips.data"))
  def zq   = lp("select * from zips where state=\"CO\" limit 3")

  final case class LP(lp: Fix[LogicalPlan]) {
    override def toString = lp.render.shows
  }
}
