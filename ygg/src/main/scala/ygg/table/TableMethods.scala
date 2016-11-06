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
import scala.math.{ min, max }

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

  private implicit def tableMethods(table: Table): TableMethods[Table] = companion tableMethods table

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
    * Sorts the KV table by ascending or descending order of a transformation
    * applied to the rows.
    *
    * @param key The transspec to use to obtain the values to sort on
    * @param order Whether to sort ascending or descending
    */
  def sort(key: TransSpec1, order: DesiredSortOrder): M[Table]

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
  // def cross(that: Table)(spec: TransSpec2): Table

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
  def mapWithSameSize(f: EndoA[NeedSlices]): Table
  def paged(limit: Int): Table
  def partitionMerge(partitionBy: TransSpec1)(f: Table => M[Table]): M[Table]
  def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]]
  def schemas: M[Set[JType]]
  def toArray[A](implicit tpe: CValueType[A]): Table
  def toJson: M[Stream[JValue]]

  def withProjections(ps: Map[Path, Projection]): Table

  def canonicalize(length: Int): Table = canonicalize(length, length)
  def slicesStream: Stream[Slice]  = slices.toStream.value

  def toJsonString: String      = toJValues mkString "\n"
  def toVector: Vector[JValue]  = toJValues.toVector
  def toJValues: Stream[JValue] = slicesStream flatMap (_.toJsonElements)
  def columns: ColumnMap        = slicesStream.head.columns
  def fields: Vector[JValue]    = toVector
  def dump(): Unit              = toVector foreach println


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

      companion.fromSlices(
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
    Need(companion.fromSlices(rec(slices, t2.slices), resultSize))
  }
  /**
    * Performs a one-pass transformation of the keys and values in the table.
    * If the key transform is not identity, the resulting table will have
    * unknown sort order.
    */
  def transform(spec: TransSpec1): Table =
    mapWithSameSize(transformStream(composeSliceTransform(spec), _))

  def concat(t2: Table): Table =
    companion.fromSlices(slices ++ t2.slices, size + t2.size)

  def normalize: Table =
    mapWithSameSize(_ filter (x => !x.isEmpty))

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
  def compact(spec: TransSpec1): Table = compact(spec, AnyDefined)
  def compact(spec: TransSpec1, definedness: Definedness): Table = {
    val transes   = Leaf(Source) -> spec mapBoth composeSliceTransform
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

    companion.fromSlices(StreamT.wrapEffect(loop(slices, 0)), newSize)
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

        val lrowsPerSlice = max(1, yggConfig.maxSliceSize / rhead.size)
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
      val left  = this.canonicalize(yggConfig.minIdealSliceSize, maxLength = yggConfig.maxSliceSize)
      val right = that.canonicalize(yggConfig.minIdealSliceSize, maxLength = yggConfig.maxSliceSize)

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

    val sizeCheck = for (resultSize <- newSizeM) yield resultSize < yggConfig.maxSaneCrossSize && resultSize >= 0

    sizeCheck match {
      case Some(false) => abort(s"cannot evaluate cartesian of sets with size $size and ${that.size}")
      case _           => companion.fromSlices(StreamT(cross0(composeSliceTransform2(spec)) map (StreamT Skip _)), newSize)
    }
  }
}
