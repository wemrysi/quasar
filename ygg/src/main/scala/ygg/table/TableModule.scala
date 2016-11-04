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

import scala.Predef.$conforms
import scalaz.{ Source => _, _ }, Scalaz._, Ordering._
import ygg._, common._, data._, json._, trans._
import ygg.cf.{ Remap, Empty }
import scala.math.{ min, max }
import quasar.Data

final case class SliceId(id: Int) {
  def +(n: Int): SliceId = SliceId(id + n)
}
final case class EnormousCartesianException(left: TableSize, right: TableSize) extends RuntimeException {
  override def getMessage =
    "cannot evaluate cartesian of sets with size %s and %s".format(left, right)
}
final case class WriteState(jdbmState: JDBMState, valueTrans: SliceTransform1[_], keyTransformsWithIds: List[SliceTransform1[_] -> String])

trait TableModule {
  outer =>

  def projections: Map[Path, Projection] = Map[Path, Projection]()

  def fromSlice(slice: Slice): Table                         = new InternalTable(slice)
  def fromSlices(slices: NeedSlices, size: TableSize): Table = new ExternalTable(slices, size)

  type TableCompanion = BaseTableCompanion
  type Table          = BaseTable
  object Table extends BaseTableCompanion

  implicit def tableCompanion: ygg.table.TableCompanion[Table] = Table

  def sourcesOf(gs: GroupingSpec[Table]): Vector[GroupingSource[Table]] = gs match {
    case x: GroupingSource[Table]                => Vector(x)
    case GroupingAlignment(_, _, left, right, _) => sourcesOf(left) ++ sourcesOf(right)
  }

  def fromJson(values: Seq[JValue]): Table = fromJson(values, None)
  def fromJson(values: Seq[JValue], maxSliceSize: Option[Int]): Table = {
    val sliceSize = maxSliceSize getOrElse yggConfig.maxSliceSize

    def makeSlice(sampleData: Stream[JValue]): Slice -> Stream[JValue] = {
      @tailrec def buildColArrays(from: Stream[JValue], into: ArrayColumnMap, sliceIndex: Int): ArrayColumnMap -> Int = from match {
        case jv #:: xs => buildColArrays(xs, Slice.withIdsAndValues(jv, into, sliceIndex, sliceSize), sliceIndex + 1)
        case _         => (into, sliceIndex)
      }
      val (prefix, suffix) = sampleData splitAt sliceSize
      val (refs, size)     = buildColArrays(prefix.toStream, Map(), 0)
      val slice            = Slice(size, refs)

      slice -> suffix
    }

    fromSlices(
      unfoldStream(values.toStream)(evts => Need(evts.nonEmpty option makeSlice(evts))),
      ExactSize(values.length)
    )
  }

  trait BaseTableCompanion extends ygg.table.TableCompanion[Table] {
    def load(table: Table, tpe: JType): M[Table] = {
      val reduced = table reduce new CReducer[Set[Path]] {
        def reduce(schema: CSchema, range: Range): Set[Path] = schema columns JTextT flatMap {
          case s: StrColumn => range collect { case i if s isDefinedAt i => Path(s(i)) }
          case _            => Set()
        }
      }
      reduced map { paths =>
        val projs = paths.toList flatMap projections.get
        Table(
          projs foldMap (_ getBlockStreamForType tpe),
          ExactSize(projs.foldMap(_.length)(Monoid[Long]))
        )
      }
    }

    def newInternalTable(slice: Slice): InternalTable                = new InternalTable(slice)
    def newExternalTable(slices: NeedSlices, size: TableSize): Table = new ExternalTable(slices, size)
    def empty: Table                                                 = Table(emptyStreamT(), ExactSize(0))
    def apply(slices: NeedSlices, size: TableSize): Table            = fromSlices(slices, size)
    def fromJson(data: Seq[JValue]): Table                           = outer fromJson data

    def constSliceTable[A: CValueType](vs: Array[A], mkColumn: Array[A] => Column): Table = Table(
      singleStreamT(Slice(vs.length, columnMap(ColumnRef.id(CValueType[A]) -> mkColumn(vs)))),
      ExactSize(vs.length)
    )
    def constSingletonTable(singleType: CType, column: Column): Table = Table(
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

    /**
      * Merge controls the iteration over the table of group key values.
      */
    def merge(grouping: GroupingSpec[Table])(body: (RValue, GroupId => M[Table]) => M[Table]): M[Table] = {
      import GroupKeySpec.{ dnf, toVector }

      type Key       = Seq[RValue]
      type KeySchema = Seq[CPathField]

      def sources(spec: GroupKeySpec): Seq[GroupKeySpecSource] = (spec: @unchecked) match {
        case GroupKeySpecAnd(left, right) => sources(left) ++ sources(right)
        case src: GroupKeySpecSource      => Vector(src)
      }

      def mkProjections(spec: GroupKeySpec) =
        toVector(dnf(spec)) map (sources(_) map (s => s.key -> s.spec))

      case class IndexedSource(groupId: GroupId, index: TableIndex, keySchema: KeySchema)

      (for {
        source              <- sourcesOf(grouping)
        groupKeyProjections <- mkProjections(source.groupKeySpec)
        disjunctGroupKeyTransSpecs = groupKeyProjections.map { case (key, spec) => spec }
      } yield {
        TableIndex.createFromTable(source.table.asInstanceOf[Table], disjunctGroupKeyTransSpecs, source.targetTrans.getOrElse(TransSpec1.Id)).map { index =>
          IndexedSource(source.groupId, index, groupKeyProjections.map(_._1))
        }
      }).sequence.flatMap { sourceKeys =>
        val fullSchema = sourceKeys.flatMap(_.keySchema).distinct

        val indicesGroupedBySource = sourceKeys.groupBy(_.groupId).mapValues(_.map(y => (y.index, y.keySchema)).toVector).values.toVector

        def unionOfIntersections(indicesGroupedBySource: Seq[Seq[TableIndex -> KeySchema]]): Set[Key] = {
          def allSourceDNF[T](l: Seq[Seq[T]]): Seq[Seq[T]] = l match {
            case Seq()       => Seq()
            case hd +: Seq() => hd.map(Seq(_))
            case hd +: tl    => hd flatMap (disjunctHd => allSourceDNF(tl) map (disjunctTl => disjunctHd +: disjunctTl))
          }
          def normalizedKeys(index: TableIndex, keySchema: KeySchema): Set[Key] = {
            val schemaMap = for (k <- fullSchema) yield keySchema.indexOf(k)
            for (key       <- index.getUniqueKeys)
              yield for (k <- schemaMap) yield if (k == -1) CUndefined else key(k)
          }

          def intersect(keys0: Set[Key], keys1: Set[Key]): Set[Key] = {
            def consistent(key0: Key, key1: Key): Boolean =
              (key0 zip key1).forall {
                case (k0, k1) => k0 == k1 || k0 == CUndefined || k1 == CUndefined
              }

            def merge(key0: Key, key1: Key): Key =
              (key0 zip key1).map {
                case (k0, CUndefined) => k0
                case (_, k1)          => k1
              }

            // TODO: This "mini-cross" is much better than the
            // previous mega-cross. However in many situations we
            // could do even less work. Consider further optimization
            // (e.g. when one key schema is a subset of the other).

            // Relatedly it might make sense to topologically sort the
            // Indices by their keyschemas so that we end up intersecting
            // key with their subset.
            keys0.flatMap { key0 =>
              keys1.flatMap(key1 => if (consistent(key0, key1)) Some(merge(key0, key1)) else None)
            }
          }

          allSourceDNF(indicesGroupedBySource).foldLeft(Set.empty[Key]) {
            case (acc, intersection) =>
              val hd = normalizedKeys(intersection.head._1, intersection.head._2)
              acc | intersection.tail.foldLeft(hd) {
                case (keys0, (index1, schema1)) =>
                  val keys1 = normalizedKeys(index1, schema1)
                  intersect(keys0, keys1)
              }
          }
        }

        def jValueFromGroupKey(key: Seq[RValue], cpaths: Seq[CPathField]): RValue = {
          val items = (cpaths zip key).map(t => (t._1.name, t._2))
          RObject(items.toMap)
        }

        val groupKeys: Set[Key] = unionOfIntersections(indicesGroupedBySource)

        // given a groupKey, return an M[Table] which represents running
        // the evaluator on that subgroup.
        def evaluateGroupKey(groupKey: Key): M[Table] = {
          val groupKeyTable = jValueFromGroupKey(groupKey, fullSchema)

          def map(gid: GroupId): M[Table] = {
            val subTableProjections = (sourceKeys
              .filter(_.groupId == gid)
              .map { indexedSource =>
                val keySchema           = indexedSource.keySchema
                val projectedKeyIndices = for (k <- fullSchema) yield keySchema.indexOf(k)
                (indexedSource.index, projectedKeyIndices, groupKey)
              })
              .toList

            Need(TableIndex.joinSubTables(subTableProjections).normalize) // TODO: normalize necessary?
          }

          body(groupKeyTable, map)
        }

        // TODO: this can probably be done as one step, but for now
        // it's probably fine.
        val tables: StreamT[Need, Table] = StreamT.unfoldM(groupKeys.toList) {
          case k :: ks => evaluateGroupKey(k).map(t => some(t -> ks))
          case Nil     => Need(None)
        }

        Need(Table(tables flatMap (_.slices), UnknownSize))
      }
    }

    /// Utility Methods ///

    def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): Table = {
      val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)

      def makeSlice(data: Stream[RValue]): Slice -> Stream[RValue] =
        data splitAt sliceSize leftMap (Slice fromRValues _)

      Table(
        unfoldStream(values)(events => Need(events.nonEmpty option makeSlice(events.toStream))),
        ExactSize(values.length)
      )
    }
  }

  sealed abstract class BaseTable(val slices: NeedSlices, val size: TableSize) extends ygg.table.Table {
    self: Table =>

    type Table = outer.Table

    override def toString = s"Table(_, $size)"
    def toJsonString: String = toJValues mkString "\n"

    /**
      * Forces a table to an external table, possibly de-optimizing it.
      */
    def toExternalTable(): ExternalTable = new outer.ExternalTable(slices, size)

    def sort(key: TransSpec1, order: DesiredSortOrder): M[Table]    = companion.sort[Need](self, key, order)
    def toInternalTable(limit: Int): ExternalTable \/ InternalTable = companion.toInternalTable(self, limit).bimap(x => fixTable[ExternalTable](x), x => fixTable[InternalTable](x))

    def toInternalTable(): ExternalTable \/ InternalTable   = toInternalTable(yggConfig.maxSliceSize)
    def mapWithSameSize(f: NeedSlices => NeedSlices): Table = Table(f(slices), size)
    def load(tpe: JType): M[Table]                          = companion.load(this, tpe)
    def compact(spec: TransSpec1): Table                    = compact(spec, AnyDefined)
    def slicesStream: Stream[Slice]                         = slices.toStream.value
    def columns: ColumnMap                                  = slicesStream.head.columns
    def toVector: Vector[JValue]                            = toJValues.toVector
    def toJValues: Stream[JValue]                           = slicesStream flatMap (_.toJsonElements)
    def toDataStream                                        = toJValues map jvalueToData

    def toData: Data = toDataStream match {
      case Seq()  => Data.NA
      case Seq(d) => d
      case xs     => Data.Arr(xs.toList)
    }

    def companion                                                      = Table
    def sample(sampleSize: Int, specs: Seq[TransSpec1]): M[Seq[Table]] = Sampling.sample[Table](self, sampleSize, specs)

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

    def compact(spec: TransSpec1, definedness: Definedness): Table = {
      val transes   = Leaf(Source) -> spec mapBoth composeSliceTransform
      val compacted = transes.fold((t1, t2) => (t1 zip t2)((s1, s2) => s1.compact(s2, definedness)))

      mapWithSameSize(transformStream(compacted, _)).normalize
    }

    /**
      * Performs a one-pass transformation of the keys and values in the table.
      * If the key transform is not identity, the resulting table will have
      * unknown sort order.
      */
    def transform(spec: TransSpec1): Table =
      mapWithSameSize(transformStream(composeSliceTransform(spec), _))

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

    def concat(t2: Table): Table = Table(
      slices ++ t2.slices,
      TableSize(size.maxSize + t2.size.maxSize)
    )

    /**
      * Zips two tables together in their current sorted order.
      * If the tables are not normalized first and thus have different slices sizes,
      * then since the zipping is done per slice, this can produce a result that is
      * different than if the tables were normalized.
      */
    def zip(t2: Table): M[Table] = {
      def rec(slices1: NeedSlices, slices2: NeedSlices): NeedSlices = {
        StreamT(slices1.uncons flatMap {
          case Some((head1, tail1)) =>
            slices2.uncons map {
              case Some((head2, tail2)) =>
                StreamT.Yield(head1 zip head2, rec(tail1, tail2))
              case None =>
                StreamT.Done
            }

          case None =>
            Need(StreamT.Done)
        })
      }

      val resultSize = EstimateSize(0, min(size.maxSize, t2.size.maxSize))
      Need(Table(rec(slices, t2.slices), resultSize))

      // todo investigate why the code below makes all of RandomLibSpecs explode
      // val resultSlices = Apply[({ type l[a] = StreamT[M, a] })#l].zip.zip(slices, t2.slices) map { case (s1, s2) => s1.zip(s2) }
      // Table(resultSlices, resultSize)
    }

    def toArray[A](implicit tpe: CValueType[A]): Table = mapWithSameSize(_ map (_.toArray[A]))

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
        case slices =>
          val slice = Slice.concat(slices)
          if (slices.size > (slice.size / yggConfig.smallSliceSize)) {
            slice.materialized // Deal w/ lots of small slices by materializing them.
          } else {
            slice
          }
      }

      def step(sliceSize: Int, acc: List[Slice], stream: NeedSlices): M[StreamT.Step[Slice, NeedSlices]] = {
        stream.uncons flatMap {
          case Some((head, tail)) =>
            if (head.size == 0) {
              // Skip empty slices.
              step(sliceSize, acc, tail)

            } else if (sliceSize + head.size >= minLength) {
              // We emit a slice, but the last slice added may fall on a stream boundary.
              val splitAt = min(head.size, maxLength - sliceSize)
              if (splitAt < head.size) {
                val (prefix, suffix) = head.split(splitAt)
                val slice            = concat(prefix :: acc)
                Need(StreamT.Yield(slice, StreamT(step(0, Nil, suffix :: tail))))
              } else {
                val slice = concat(head :: acc)
                Need(StreamT.Yield(slice, StreamT(step(0, Nil, tail))))
              }

            } else {
              // Just keep swimming (aka accumulating).
              step(sliceSize + head.size, head :: acc, tail)
            }

          case None =>
            if (sliceSize > 0) {
              Need(StreamT.Yield(concat(acc), emptyStreamT()))
            } else {
              Need(StreamT.Done)
            }
        }
      }

      mapWithSameSize(ss => StreamT(step(0, Nil, ss)))
    }

    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): Table =
      companion.cogroup(self, leftKey, rightKey, that)(leftResultTrans, rightResultTrans, bothResultTrans)

    /**
      * Performs a full cartesian cross on this table with the specified table,
      * applying the specified transformation to merge the two tables into
      * a single table.
      */
    def cross(that: Table)(spec: TransSpec2): Table = {
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

        left.slices.uncons flatMap {
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
        case _           => Table(StreamT(cross0(composeSliceTransform2(spec)) map (StreamT Skip _)), newSize)
      }
    }

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

        Table(
          StreamT.wrapEffect(Need(this) map (sorted => stream(id.initial -> filter.initial, sorted.slices))),
          EstimateSize(0L, size.maxSize)
        )
      }

      distinct0(SliceTransform.identity(None: Option[Slice]), composeSliceTransform(spec))
    }

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
        WrapObject(Leaf(Source), "1")
      )

      this.transform(keyTrans).compact(TransSpec1.Id).slices.uncons map {
        case Some((head, tail)) =>
          Table(stepPartition(head, 0, tail), UnknownSize)
        case None =>
          Table.empty
      }
    }

    def normalize: Table = mapWithSameSize(_ filter (x => !x.isEmpty))

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

    def toStrings: Need[Stream[String]] = toEvents(_ toString _)
    def toJson: Need[Stream[JValue]]    = toEvents(_ toJson _)

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
  final class InternalTable(val slice: Slice) extends BaseTable(singleStreamT(slice), ExactSize(slice.size)) with ygg.table.InternalTable {
    override def force: M[Table]          = Need(this)
    override def paged(limit: Int): Table = this

    def takeRange(start: Long, len: Long): Table = (
      if (start > Int.MaxValue)
        new outer.InternalTable(Slice.empty)
      else
        new outer.InternalTable(slice.takeRange(start.toInt, len.toInt))
    )
  }

  final class ExternalTable(slices: NeedSlices, size: TableSize) extends BaseTable(slices, size) with ygg.table.ExternalTable {
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

      Table(StreamT.wrapEffect(loop(slices, 0)), newSize)
    }

    def externalToInternal(limit: Int): ExternalTable \/ InternalTable = {
      def acc(slices: NeedSlices, buffer: List[Slice], size: Long): Need[ExternalTable \/ InternalTable] = {
        def finishInt() = Slice.concat(buffer.reverse)
        slices.uncons flatMap {
          case Some((head, tail)) =>
            val nextSize    = size + head.size
            val next        = head :: buffer
            def finishExt() = Slice.concat(next.reverse) :: tail

            if (nextSize > limit.toLong)
              Need(-\/(new outer.ExternalTable(finishExt, this.size isAtLeast nextSize)))
            else
              acc(tail, next, nextSize)

          case None =>
            Need(\/-(new outer.InternalTable(finishInt)))
        }
      }

      acc(slices, Nil, 0L).value
    }
  }
}
