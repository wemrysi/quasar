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

import ygg._, common._, data._, json._, trans._
import scalaz._, Scalaz._, Ordering._

final case class SliceId(id: Int) {
  def +(n: Int): SliceId = SliceId(id + n)
}
final case class EnormousCartesianException(left: TableSize, right: TableSize) extends RuntimeException {
  override def getMessage =
    "cannot evaluate cartesian of sets with size %s and %s".format(left, right)
}
final case class WriteState(jdbmState: JDBMState, valueTrans: SliceTransform1[_], keyTransformsWithIds: List[SliceTransform1[_] -> String])

sealed abstract class BaseTable(val slices: NeedSlices, val size: TableSize) extends ygg.table.Table {
  self =>

  override def toString = s"Table(_, $size)"

  def sort(key: TransSpec1, order: DesiredSortOrder): M[Table] = companion.sort[Need](self, key, order)

  def mapWithSameSize(f: EndoA[NeedSlices]): Table             = companion.fromSlices(f(slices), size)
  def load(tpe: JType): M[Table]                               = companion.load(this, tpe)
  def sample(size: Int, specs: Seq[TransSpec1]): M[Seq[Table]] = Sampling.sample(self, size, specs)

  /**
    * Folds over the table to produce a single value (stored in a singleton table).
    */
  def reduce[A](reducer: CReducer[A])(implicit monoid: Monoid[A]): Need[A] = {
    def rec(stream: StreamT[Need, A], acc: A): Need[A] = stream.uncons flatMap {
      case Some((head, tail)) => rec(tail, head |+| acc)
      case None               => Need(acc)
    }
    rec(
      slices map (s => reducer.reduce(new CSchema(s.columns.keySet, s logicalColumns _), 0 until s.size)),
      monoid.zero
    )
  }

  def force: M[Table] = {
    def loop(slices: NeedSlices, acc: List[Slice], size: Long): Need[List[Slice] -> Long] = slices.uncons flatMap {
      case Some((slice, tail)) if slice.size > 0 => loop(tail, slice.materialized :: acc, size + slice.size)
      case Some((_, tail))                       => loop(tail, acc, size)
      case None                                  => Need(acc.reverse -> size)
    }
    val former = new (Id.Id ~> Need) { def apply[A](a: Id.Id[A]): Need[A] = Need(a) }
    loop(slices, Nil, 0L) map {
      case (stream, size) =>
        Table(StreamT.fromIterable(stream).trans(former), ExactSize(size))
    }
  }

  def paged(limit: Int): Table = mapWithSameSize(slices =>
    slices flatMap (slice =>
      StreamT.unfoldM(0)(idx =>
        Need(idx < slice.size option (slice.takeRange(idx, limit) -> (idx + limit)))
      )
    )
  )

  def toArray[A](implicit tpe: CValueType[A]): Table = mapWithSameSize(_ map (_.toArray[A]))

  def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table =
    companion.cogroup(self, leftKey, rightKey, that)(leftResultTrans, rightResultTrans, bothResultTrans)

  /**
    * In order to call partitionMerge, the table must be sorted according to
    * the values specified by the partitionBy transspec.
    */
  def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table] = {
    // Find the first element that compares LT
    @tailrec def findEnd(compare: Int => Ordering, imin: Int, imax: Int): Int = {
      val imid = imin + (imax - imin) / 2

      (compare(imin), compare(imid), compare(imax)) match {
        case (LT, _, LT)  => imin
        case (EQ, _, EQ)  => imax + 1
        case (EQ, LT, LT) => findEnd(compare, imin, imid - 1)
        case (EQ, EQ, LT) => findEnd(compare, imid, imax - 1)
        case _            => abort("Inputs to partitionMerge not sorted.")
      }
    }

    def subTable(comparatorGen: Slice => (Int => Ordering), slices: NeedSlices): M[Table] = {
      def subTable0(slices: NeedSlices, subSlices: NeedSlices, size: Int): M[Table] = {
        slices.uncons flatMap {
          case Some((head, tail)) =>
            val headComparator = comparatorGen(head)
            val spanEnd        = findEnd(headComparator, 0, head.size - 1)
            if (spanEnd < head.size) {
              Need(Table(subSlices ++ singleStreamT(head take spanEnd), ExactSize(size + spanEnd)))
            } else {
              subTable0(tail, subSlices ++ singleStreamT(head), size + head.size)
            }

          case None =>
            Need(Table(subSlices, ExactSize(size)))
        }
      }

      subTable0(slices, emptyStreamT(), 0)
    }

    def dropAndSplit(comparatorGen: Slice => (Int => Ordering), slices: NeedSlices, spanStart: Int): NeedSlices = StreamT.wrapEffect {
      slices.uncons map {
        case Some((head, tail)) =>
          val headComparator = comparatorGen(head)
          val spanEnd        = findEnd(headComparator, spanStart, head.size - 1)
          if (spanEnd < head.size) {
            stepPartition(head, spanEnd, tail)
          } else {
            dropAndSplit(comparatorGen, tail, 0)
          }

        case None =>
          emptyStreamT()
      }
    }

    def stepPartition(head: Slice, spanStart: Int, tail: NeedSlices): NeedSlices = {
      val comparatorGen = (s: Slice) => {
        val rowComparator = Slice.rowComparatorFor(head, s) { s0 =>
          s0.columns.keys collect {
            case ColumnRef(path @ CPath(CPathField("0"), _ @_ *), _) => path
          }
        }

        (i: Int) =>
          rowComparator.compare(spanStart, i)
      }

      val groupTable                       = subTable(comparatorGen, head.drop(spanStart) :: tail)
      val groupedM                         = groupTable.map(_ transform root.`1`).flatMap(f)
      val groupedStream: NeedSlices = StreamT.wrapEffect(groupedM.map(_.slices))

      groupedStream ++ dropAndSplit(comparatorGen, head :: tail, spanStart)
    }

    val keyTrans = OuterObjectConcat(
      WrapObject(partitionBy, "0"),
      WrapObject(Leaf(trans.Source), "1")
    )

    this.transform(keyTrans).compact(TransSpec1.Id).slices.uncons map {
      case Some((head, tail)) =>
        Table(stepPartition(head, 0, tail), UnknownSize)
      case None =>
        Table.empty
    }
  }

  def schemas: M[Set[JType]] = {
    // Returns true iff masks contains an array equivalent to mask.
    def contains(masks: List[RawBitSet], mask: Array[Int]): Boolean = {
      @tailrec
      def equal(x: Array[Int], y: Array[Int], i: Int): Boolean = (
           i >= x.length
        || x(i) == y(i) && equal(x, y, i + 1)
      )

      @tailrec
      def loop(xs: List[RawBitSet], y: Array[Int]): Boolean = xs match {
        case x :: xs if x.length == y.length && equal(x.bits, y, 0) => true
        case _ :: xs                                                => loop(xs, y)
        case Nil                                                    => false
      }

      loop(masks, mask)
    }

    def isZero(x: Array[Int]): Boolean = {
      @tailrec def loop(i: Int): Boolean = i < 0 || x(i) == 0 && loop(i - 1)
      loop(x.length - 1)
    }

    // Constructs a schema from a set of defined ColumnRefs. Metadata is
    // ignored and there can be no unions. The set of ColumnRefs must all be
    // defined and hence must create a valid JSON object.
    def mkSchema(cols: List[ColumnRef]): Option[JType] = {
      def leafType(ctype: CType): JType = ctype match {
        case CBoolean               => JBooleanT
        case CLong | CDouble | CNum => JNumberT
        case CString                => JTextT
        case CDate                  => JDateT
        case CPeriod                => JPeriodT
        case CArrayType(elemType)   => leafType(elemType)
        case CEmptyObject           => JType.Object()
        case CEmptyArray            => JType.Array()
        case CNull                  => JNullT
        case CUndefined             => abort("not supported")
      }

      def fresh(paths: Seq[CPathNode], leaf: JType): Option[JType] = paths match {
        case CPathField(field) +: paths => fresh(paths, leaf) map (tpe => JType.Object(field -> tpe))
        case CPathIndex(i) +: paths     => fresh(paths, leaf) map (tpe => JType.Indexed(i -> tpe))
        case CPathArray +: paths        => fresh(paths, leaf) map (tpe => JArrayHomogeneousT(tpe))
        case CPathMeta(field) +: _      => None
        case Seq()                      => Some(leaf)
      }

      def merge(schema: Option[JType], paths: Seq[CPathNode], leaf: JType): Option[JType] = (schema, paths) match {
        case (Some(JObjectFixedT(fields)), CPathField(field) +: paths) =>
          merge(fields get field, paths, leaf) map { tpe =>
            JObjectFixedT(fields + (field -> tpe))
          } orElse schema
        case (Some(JArrayFixedT(indices)), CPathIndex(idx) +: paths) =>
          merge(indices get idx, paths, leaf) map { tpe =>
            JArrayFixedT(indices + (idx -> tpe))
          } orElse schema
        case (None, paths) =>
          fresh(paths, leaf)
        case (jtype, paths) =>
          abort("Invalid schema.") // This shouldn't happen for any real data.
      }

      cols.foldLeft(None: Option[JType]) {
        case (schema, ColumnRef(cpath, ctype)) =>
          merge(schema, cpath.nodes, leafType(ctype))
      }
    }

    // Collects all possible schemas from some slices.
    def collectSchemas(schemas: Set[JType], slices: NeedSlices): Need[Set[JType]] = {
      def buildMasks(cols: Array[Column], sliceSize: Int): List[RawBitSet] = {
        import java.util.Arrays.copyOf
        val mask = RawBitSet.create(cols.length)

        @tailrec def build0(row: Int, masks: List[RawBitSet]): List[RawBitSet] = {
          if (row < sliceSize) {
            mask.clear()

            var j = 0
            while (j < cols.length) {
              if (cols(j) isDefinedAt row) mask.set(j)
              j += 1
            }

            val next = (
              if (!contains(masks, mask.bits) && !isZero(mask.bits))
                new RawBitSet(copyOf(mask.bits, mask.length)) :: masks
              else
                masks
            )

            build0(row + 1, next)
          }
          else masks
        }

        build0(0, Nil)
      }

      slices.uncons flatMap {
        case Some((slice, slices)) =>
          val (refs0, cols0) = slice.columns.unzip

          val masks                        = buildMasks(cols0.toArray, slice.size)
          val refs: List[ColumnRef -> Int] = refs0.zipWithIndex.toList
          val next = masks flatMap { schemaMask =>
            mkSchema(refs collect { case (ref, i) if schemaMask.get(i) => ref })
          }

          collectSchemas(schemas ++ next, slices)

        case None =>
          Need(schemas)
      }
    }

    collectSchemas(Set.empty, slices)
  }

  def toStrings: Need[Stream[String]]                       = toEvents(_ toString _)
  def toJson: Need[Stream[JValue]]                          = toEvents(_ toJson _)
  def projections: Map[Path, Projection]                    = Map()
  def withProjections(ps: Map[Path, Projection]): BaseTable = new ProjectionsTable(this, projections ++ ps)

  private def toEvents[A](f: (Slice, RowId) => Option[A]): Need[Stream[A]] = (
    (self compact root.spec).slices.toStream map (stream =>
      stream flatMap (slice =>
        0 until slice.size flatMap (i =>
          f(slice, i)
        )
      )
    )
  )
}

/**
  * `InternalTable`s are tables that are *generally* small and fit in a single
  * slice and are completely in-memory. Because they fit in memory, we are
  * allowed more optimizations when doing things like joins.
  */

final class InternalTable(val slice: Slice)                                                          extends BaseTable(singleStreamT(slice), ExactSize(slice.size))
final class ProjectionsTable(val underlying: Table, override val projections: Map[Path, Projection]) extends BaseTable(underlying.slices, underlying.size)
final class ExternalTable(slices: NeedSlices, size: TableSize)                                       extends BaseTable(slices, size)
