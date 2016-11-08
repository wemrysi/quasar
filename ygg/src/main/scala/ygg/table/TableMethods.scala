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

import ygg._, common._, json._, trans._
import scala.math.{ min, max }
import scalaz._, Scalaz._

class TableMethods[Table: TableRep](val self: Table) {
  type M[+X] = Need[X]

  def companion                                         = companionOf[Table]
  def slices: NeedSlices                                = companion slicesOf self
  def size: TableSize                                   = companion sizeOf self
  def projections: Map[Path, Projection]                = companion projectionsOf self
  def withProjections(ps: Map[Path, Projection]): Table = companion.withProjections(self, ps)

  private def makeTable(slices: NeedSlices, size: TableSize): Table             = companion.fromSlices(slices, size)
  private def needTable(slices: => NeedSlices, size: => TableSize): Need[Table] = Need(makeTable(slices, size))

  def sample(size: Int, specs: Seq[TransSpec1]): M[Seq[Table]] = Sampling.sample(self, size, specs)

  /**
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param key The transspec to use to obtain the values to sort on
    * @param order Whether to sort ascending or descending
    */
  def sort(key: TransSpec1, order: DesiredSortOrder): Table =
    WriteTable[Table].groupByN(self, Seq(key), `.`, order, unique = false).headOption getOrElse companion.empty

  def sort(key: TransSpec1): Table =
    sort(key, SortAscending)

  /**
    * Cogroups this table with another table, using equality on the specified
    * transformation on rows of the table.
    */
  def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table =
    CogroupTable[Table].cogroup(
      CogroupTable.CogroupSide(self, leftKey, leftResultTrans),
      CogroupTable.CogroupSide(that, rightKey, rightResultTrans),
      bothResultTrans
    )

  def align(sourceL: Table, alignL: TransSpec1, sourceR: Table, alignR: TransSpec1): PairOf[Table] =
    AlignTable(sourceL, alignL, sourceR, alignR)

  def columns: ColumnMap          = slicesStream.head.columns
  def concat(t2: Table): Table    = makeTable(slices ++ t2.slices, size + t2.size)
  def dump(): Unit                = toVector foreach println
  def fields: Vector[JValue]      = toVector
  def normalize: Table            = mapWithSameSize(_ filter (x => !x.isEmpty))
  def slicesStream: Stream[Slice] = slices.toStream.value
  def toJValues: Stream[JValue]   = slicesStream flatMap (_.toJValues)
  def toJsonString: String        = toJValues mkString "\n"
  def toVector: Vector[JValue]    = toJValues.toVector
  def toSeq: Seq[JValue]          = toVector

  /** Test compat */
  def toJson: Need[Stream[JValue]] = Need(toJValues)

  /**
    * Yields a new table with distinct rows. Assumes this table is sorted.
    */
  def distinct(spec: TransSpec1): Table = {
    def distinct0[T](id: SliceTransform1[Option[Slice]], filter: SliceTransform1[T]): Table = {
      def stream(state: (Option[Slice], T), slices: NeedSlices): NeedSlices = StreamT(
        for {
          head <- slices.uncons

          back <- {
            head map {
              case (s, sx) => {
                for {
                  pairPrev <- id.f(state._1, s)
                  // TODO use an Applicative
                  pairNext <- filter.f(state._2, s)
                } yield {
                  val (prevFilter, cur)  = pairPrev
                  val (nextT, curFilter) = pairNext
                  val next               = cur.distinct(prevFilter, curFilter)

                  StreamT.Yield(next, stream((if (next.size > 0) Some(curFilter) else prevFilter, nextT), sx))
                }
              }
            } getOrElse {
              Need(StreamT.Done)
            }
          }
        } yield back
      )

      makeTable(
        StreamT.wrapEffect(Need(this) map (sorted => stream(id.initial -> filter.initial, sorted.slices))),
        EstimateSize(0L, size.maxSize)
      )
    }

    distinct0(SliceTransform.identity(None: Option[Slice]), composeSliceTransform(spec))
  }

  /**
    * Zips two tables together in their current sorted order.
    * If the tables are not normalized first and thus have different slices sizes,
    * then since the zipping is done per slice, this can produce a result that is
    * different than if the tables were normalized.
    */
  def zip(t2: Table): M[Table] = {
    def rec(slices1: NeedSlices, slices2: NeedSlices): NeedSlices = StreamT(
      slices1.uncons flatMap {
        case None                 => Need(StreamT.Done)
        case Some((head1, tail1)) =>
          slices2.uncons map {
            case Some((head2, tail2)) => StreamT.Yield(head1 zip head2, rec(tail1, tail2))
            case None                 => StreamT.Done
          }
      }
    )

    val resultSize = EstimateSize(0, min(size.maxSize, t2.size.maxSize))
    needTable(rec(slices, t2.slices), resultSize)
  }
  /**
    * Performs a one-pass transformation of the keys and values in the table.
    * If the key transform is not identity, the resulting table will have
    * unknown sort order.
    */
  def transform(spec: TransSpec1): Table =
    mapWithSameSize(transformStream(composeSliceTransform(spec), _))

  private def transformStream[A](sliceTransform: SliceTransform1[A], slices: NeedSlices): NeedSlices = {
    def stream(state: A, slices: NeedSlices): NeedSlices = StreamT(
      for {
        head <- slices.uncons

        back <- {
          head map {
            case (s, sx) => {
              sliceTransform.f(state, s) map {
                case (nextState, s0) =>
                  StreamT.Yield(s0, stream(nextState, sx))
              }
            }
          } getOrElse {
            Need(StreamT.Done)
          }
        }
      } yield back
    )

    stream(sliceTransform.initial, slices)
  }

  /**
    * Removes all rows in the table for which definedness is satisfied
    * Remaps the indicies.
    */

  def compact(): Table                 = compact(`.`)
  def compact(spec: TransSpec1): Table = compact(spec, AnyDefined)
  def compact(spec: TransSpec1, definedness: Definedness): Table = {
    val transes   = root.spec -> spec mapBoth composeSliceTransform
    val compacted = transes.fold((t1, t2) => (t1 zip t2)((s1, s2) => s1.compact(s2, definedness)))

    mapWithSameSize(transformStream(compacted, _)).normalize
  }

  def takeRange(startIndex: Long, numberToTake: Long): Table = {
    def loop(stream: NeedSlices, readSoFar: Long): M[NeedSlices] = stream.uncons flatMap {
      // Prior to first needed slice, so skip
      case Some((head, tail)) if (readSoFar + head.size) < (startIndex + 1) => loop(tail, readSoFar + head.size)
      // Somewhere in between, need to transition to splitting/reading
      case Some(_) if readSoFar < (startIndex + 1) => inner(stream, 0, (startIndex - readSoFar).toInt)
      // Read off the end (we took nothing)
      case _ => Need(emptyStreamT())
    }

    def inner(stream: NeedSlices, takenSoFar: Long, sliceStartIndex: Int): M[NeedSlices] = stream.uncons flatMap {
      case Some((head, tail)) if takenSoFar < numberToTake => {
        val needed = head.takeRange(sliceStartIndex, (numberToTake - takenSoFar).toInt)
        inner(tail, takenSoFar + (head.size - (sliceStartIndex)), 0).map(needed :: _)
      }
      case _ => Need(emptyStreamT())
    }
    def calcNewSize(current: Long): Long = min(max(current - startIndex, 0), numberToTake)

    val newSize = size match {
      case ExactSize(sz)            => ExactSize(calcNewSize(sz))
      case EstimateSize(sMin, sMax) => TableSize(calcNewSize(sMin), calcNewSize(sMax))
      case UnknownSize              => UnknownSize
      case InfiniteSize             => InfiniteSize
    }

    makeTable(StreamT.wrapEffect(loop(slices, 0)), newSize)
  }

  /**
    * Performs a full cartesian cross on this table with the specified table,
    * applying the specified transformation to merge the two tables into
    * a single table.
    */
  def cross(that: Table)(spec: TransSpec2): Table = {
    import ygg.cf.{ Remap, Empty }

    def cross0[A](transform: SliceTransform2[A]): M[NeedSlices] = {
      case class CrossState(a: A, position: Int, tail: NeedSlices)

      def crossBothSingle(lhead: Slice, rhead: Slice)(a0: A): M[A -> NeedSlices] = {

        // We try to fill out the slices as much as possible, so we work with
        // several rows from the left at a time.

        val lrowsPerSlice = max(1, companion.maxSliceSize / rhead.size)
        val sliceSize     = lrowsPerSlice * rhead.size

        // Note that this is still memory efficient, as the columns are re-used
        // between all slices.

        val results = (0 until lhead.size by lrowsPerSlice).foldLeft(Need((a0, List.empty[Slice]))) {
          case (accM, offset) =>
            accM flatMap {
              case (a, acc) =>
                val rows = min(sliceSize, (lhead.size - offset) * rhead.size)

                val lslice = Slice(
                  rows,
                  lhead.columns.lazyMapValues(Remap({ i =>
                    offset + (i / rhead.size)
                  })(_).get)
                )

                val rslice = Slice(
                  rows,
                  if (rhead.size == 0)
                    rhead.columns.lazyMapValues(Empty(_).get)
                  else
                    rhead.columns.lazyMapValues(Remap(_ % rhead.size)(_).get)
                )

                transform.f(a, lslice, rslice) map {
                  case (b, resultSlice) =>
                    (b, resultSlice :: acc)
                }
            }
        }

        results map {
          case (a1, slices) =>
            val sliceStream = slices.reverse.toStream
            (a1, StreamT.fromStream(Need(sliceStream)))
        }
      }

      def crossLeftSingle(lhead: Slice, right: NeedSlices)(a0: A): NeedSlices = {
        def step(state: CrossState): M[Option[Slice -> CrossState]] = {
          if (state.position < lhead.size) {
            state.tail.uncons flatMap {
              case Some((rhead, rtail0)) =>
                val lslice = Slice(rhead.size, lhead.columns.lazyMapValues(Remap(i => state.position)(_).get))

                transform.f(state.a, lslice, rhead) map {
                  case (a0, resultSlice) =>
                    Some((resultSlice, CrossState(a0, state.position, rtail0)))
                }

              case None =>
                step(CrossState(state.a, state.position + 1, right))
            }
          } else {
            Need(None)
          }
        }

        unfoldStream(CrossState(a0, 0, right))(step)
      }

      def crossRightSingle(left: NeedSlices, rhead: Slice)(a0: A): NeedSlices = {
        StreamT(left.uncons flatMap {
          case Some((lhead, ltail0)) =>
            crossBothSingle(lhead, rhead)(a0) map {
              case (a1, prefix) =>
                StreamT.Skip(prefix ++ crossRightSingle(ltail0, rhead)(a1))
            }

          case None =>
            Need(StreamT.Done)
        })
      }

      def crossBoth(ltail: NeedSlices, rtail: NeedSlices): NeedSlices = {
        // This doesn't carry the Transform's state around, so, I think it is broken.
        ltail.flatMap(crossLeftSingle(_, rtail)(transform.initial))
      }

      // We canonicalize the tables so that no slices are too small.
      val left  = this.canonicalize(companion.minIdealSliceSize, maxLength = companion.maxSliceSize)
      val right = that.canonicalize(companion.minIdealSliceSize, maxLength = companion.maxSliceSize)

      val ss = left.slices.uncons

      ss flatMap {
        case Some((lhead, ltail)) =>
          right.slices.uncons flatMap {
            case Some((rhead, rtail)) =>
              for {
                lempty <- ltail.isEmpty //TODO: Scalaz result here is negated from what it should be!
                rempty <- rtail.isEmpty

                back <- {
                  if (lempty && rempty) {
                    // both are small sets, so find the cross in memory
                    crossBothSingle(lhead, rhead)(transform.initial) map { _._2 }
                  } else if (lempty) {
                    // left side is a small set, so restart it in memory
                    Need(crossLeftSingle(lhead, rhead :: rtail)(transform.initial))
                  } else if (rempty) {
                    // right side is a small set, so restart it in memory
                    Need(crossRightSingle(lhead :: ltail, rhead)(transform.initial))
                  } else {
                    // both large sets, so just walk the left restarting the right.
                    Need(crossBoth(lhead :: ltail, rhead :: rtail))
                  }
                }
              } yield back

            case None => Need(emptyStreamT())
          }

        case None => Need(emptyStreamT())
      }
    }

    // TODO: We should be able to fully compute the size of the result above.
    val newSize = (size, that.size) match {
      case (ExactSize(l), ExactSize(r))         => TableSize(max(l, r), l * r)
      case (EstimateSize(ln, lx), ExactSize(r)) => TableSize(max(ln, r), lx * r)
      case (ExactSize(l), EstimateSize(rn, rx)) => TableSize(max(l, rn), l * rx)
      case _                                    => UnknownSize // Bail on anything else for now (see above TODO)
    }

    val newSizeM = newSize match {
      case ExactSize(s)       => Some(s)
      case EstimateSize(_, s) => Some(s)
      case _                  => None
    }

    val sizeCheck = for (resultSize <- newSizeM) yield resultSize < companion.maxSaneCrossSize && resultSize >= 0

    sizeCheck match {
      case Some(false) => abort(s"cannot evaluate cartesian of sets with size $size and ${that.size}")
      case _           => makeTable(StreamT(cross0(composeSliceTransform2(spec)) map (StreamT Skip _)), newSize)
    }
  }

  /**
    * Returns a table where each slice (except maybe the last) has slice size `length`.
    * Also removes slices of size zero. If an optional `maxLength0` size is provided,
    * then the slices need only land in the range between `length` and `maxLength0`.
    * For slices being loaded from ingest, it is often the case that we are missing a
    * few rows at the end, so we shouldn't be too strict.
    */
  def canonicalize(length: Int): Table = canonicalize(length, length)
  def canonicalize(minLength: Int, maxLength: Int): Table = {
    scala.Predef.assert(maxLength > 0 && minLength >= 0 && maxLength >= minLength, "length bounds must be positive and ordered")

    def concat(rslices: List[Slice]): Slice = rslices.reverse match {
      case Nil          => Slice.empty
      case slice :: Nil => slice
      case all          =>
        val result      = Slice concat all
        val materialize = all.size > (result.size / companion.smallSliceSize)
        // Deal w/ lots of small slices by materializing them.
        materialize.fold(result, result.materialized)
    }

    def step(sliceSize: Int, acc: List[Slice], stream: NeedSlices): M[StreamT.Step[Slice, NeedSlices]] = {
      def mkYield(hds: List[Slice], tl: NeedSlices) =
        Need(StreamT.Yield(concat(hds), StreamT(step(0, Nil, tl))))

      stream.uncons flatMap {
        case None if sliceSize > 0                                   => mkYield(acc, emptyStreamT())
        case None                                                    => Need(StreamT.Done)
        case Some((EmptySlice(), tail))                              => step(sliceSize, acc, tail) // Skip empty slices.
        case Some((head, tail)) if sliceSize + head.size < minLength => step(sliceSize + head.size, head :: acc, tail) // Keep accumulating.
        case Some((head, tail))                                      =>
          // We emit a slice, but the last slice added may fall on a stream boundary.
          min(head.size, maxLength - sliceSize) match {
            case splitAt if splitAt < head.size =>
              val (prefix, suffix) = head split splitAt
              mkYield(prefix :: acc, suffix :: tail)
            case _ =>
              mkYield(head :: acc, tail)
          }
      }
    }

    mapWithSameSize(ss => StreamT(step(0, Nil, ss)))
  }


  /**
    * In order to call partitionMerge, the table must be sorted according to
    * the values specified by the partitionBy transspec.
    */
  def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table] = {
    import Ordering._

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
              needTable(subSlices ++ singleStreamT(head take spanEnd), ExactSize(size + spanEnd))
            } else {
              subTable0(tail, subSlices ++ singleStreamT(head), size + head.size)
            }

          case None =>
            needTable(subSlices, ExactSize(size))
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

      val groupTable                = subTable(comparatorGen, head.drop(spanStart) :: tail)
      val groupedM                  = groupTable.map(_ transform root.dyn.`1`).flatMap(f)
      val groupedStream: NeedSlices = StreamT.wrapEffect(groupedM.map(_.slices))

      groupedStream ++ dropAndSplit(comparatorGen, head :: tail, spanStart)
    }

    val keyTrans = OuterObjectConcat(
      WrapObject(partitionBy, "0"),
      WrapObject(root, "1")
    )

    this.transform(keyTrans).compact(TransSpec1.Id).slices.uncons map {
      case Some((head, tail)) => makeTable(stepPartition(head, 0, tail), UnknownSize)
      case None               => companion.empty
    }
  }

  def schemas: M[Set[JType]] = {
    import data._

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

  def mapWithSameSize(f: EndoA[NeedSlices]): Table = companion.fromSlices(f(slices), size)

  /**
    * For each distinct path in the table, load all columns identified by the specified
    * jtype and concatenate the resulting slices into a new table.
    */
  def load(tpe: JType): M[Table] = companion.load(self, tpe)

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

  /**
    * Force the table to a backing store, and provice a restartable table
    * over the results.
    */
  def force: M[Table] = {
    def loop(slices: NeedSlices, acc: List[Slice], size: Long): Need[List[Slice] -> Long] = slices.uncons flatMap {
      case Some((slice, tail)) if slice.size > 0 => loop(tail, slice.materialized :: acc, size + slice.size)
      case Some((_, tail))                       => loop(tail, acc, size)
      case None                                  => Need(acc.reverse -> size)
    }
    val former = new (Id.Id ~> Need) { def apply[A](a: Id.Id[A]): Need[A] = Need(a) }
    loop(slices, Nil, 0L) map {
      case (stream, size) =>
        makeTable(StreamT.fromIterable(stream).trans(former), ExactSize(size))
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
}
