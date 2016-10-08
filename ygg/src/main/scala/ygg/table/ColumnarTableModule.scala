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
import scalaz._, Scalaz._, Ordering._
import ygg._, common._, data._, json._
import ygg.cf.{ Remap, Empty }
import scala.math.{ min, max }

final case class SliceId(id: Int) {
  def +(n: Int): SliceId = SliceId(id + n)
}

trait ColumnarTableModule extends TableModule with SliceTransforms with IndicesModule {
  outer =>

  import trans._

  type Table <: ColumnarTable
  type TableCompanion <: ColumnarTableCompanion

  def jdbmCommitInterval: Long = 200000l

  def sourcesOf(gs: GroupingSpec): Vector[GroupingSource] = gs match {
    case x: GroupingSource                       => Vector(x)
    case GroupingAlignment(_, _, left, right, _) => sourcesOf(left) ++ sourcesOf(right)
  }
  def sortedGroupingSpec(gs: GroupingSpec): Need[GroupingSpec] = gs match {
    case GroupingSource(table, idTrans, targetTrans, groupId, groupKeySpec) =>
      (table.asInstanceOf[Table]).sort(root.key) map (t => GroupingSource(t, idTrans, targetTrans, groupId, groupKeySpec))
    case GroupingAlignment(groupKeyLeftTrans, groupKeyRightTrans, left, right, alignment) =>
      (sortedGroupingSpec(left) |@| sortedGroupingSpec(right))(GroupingAlignment(groupKeyLeftTrans, groupKeyRightTrans, _, _, alignment))
  }

  trait ColumnarTableCompanion extends ygg.table.TableCompanion[outer.Table] {
    def apply(slices: NeedSlices, size: TableSize): Table

    def singleton(slice: Slice): Table

    implicit def groupIdShow: Show[GroupId] = Show.showFromToString[GroupId]

    def empty: Table = Table(emptyStreamT(), ExactSize(0))

    def constSliceTable[A: CValueType](vs: Array[A], mkColumn: Array[A] => Column): Table = Table(
      singleStreamT(Slice(vs.length, Map(ColumnRef.id(CValueType[A]) -> mkColumn(vs)))),
      ExactSize(vs.length)
    )
    def constSingletonTable(singleType: CType, column: Column): Table = Table(
      singleStreamT(Slice(1, Map(ColumnRef.id(singleType) -> column))),
      ExactSize(1)
    )

    def constBoolean(v: scSet[Boolean]): Table    = constSliceTable[Boolean](v.toArray, ArrayBoolColumn(_))
    def constLong(v: scSet[Long]): Table          = constSliceTable[Long](v.toArray, ArrayLongColumn(_))
    def constDouble(v: scSet[Double]): Table      = constSliceTable[Double](v.toArray, ArrayDoubleColumn(_))
    def constDecimal(v: scSet[BigDecimal]): Table = constSliceTable[BigDecimal](v.toArray, ArrayNumColumn(_))
    def constString(v: scSet[String]): Table      = constSliceTable[String](v.toArray, ArrayStrColumn(_))
    def constDate(v: scSet[DateTime]): Table      = constSliceTable[DateTime](v.toArray, ArrayDateColumn(_))
    def constNull: Table                          = constSingletonTable(CNull, new InfiniteColumn with NullColumn)
    def constEmptyObject: Table                   = constSingletonTable(CEmptyObject, new InfiniteColumn with EmptyObjectColumn)
    def constEmptyArray: Table                    = constSingletonTable(CEmptyArray, new InfiniteColumn with EmptyArrayColumn)

    def transformStream[A](sliceTransform: SliceTransform1[A], slices: NeedSlices): NeedSlices = {
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
      * Merge controls the iteration over the table of group key values.
      */
    def merge(grouping: GroupingSpec)(body: (RValue, GroupId => NeedTable) => NeedTable): NeedTable = {
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

        val indicesGroupedBySource = sourceKeys.groupBy(_.groupId).mapValues(_.map(y => (y.index, y.keySchema)).toSeq).values.toSeq

        def unionOfIntersections(indicesGroupedBySource: scSeq[scSeq[TableIndex -> KeySchema]]): Set[Key] = {
          def allSourceDNF[T](l: scSeq[scSeq[T]]): scSeq[scSeq[T]] = {
            l match {
              case Seq(hd) => hd.map(Seq(_))
              case Seq(hd, tl @ _ *) => {
                for {
                  disjunctHd <- hd
                  disjunctTl <- allSourceDNF(tl)
                } yield disjunctHd +: disjunctTl
              }
              case empty => empty
            }
          }

          def normalizedKeys(index: TableIndex, keySchema: KeySchema): scSet[Key] = {
            val schemaMap = for (k <- fullSchema) yield keySchema.indexOf(k)
            for (key       <- index.getUniqueKeys)
              yield for (k <- schemaMap) yield if (k == -1) CUndefined else key(k)
          }

          def intersect(keys0: scSet[Key], keys1: scSet[Key]): scSet[Key] = {
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

        // given a groupKey, return an NeedTable which represents running
        // the evaluator on that subgroup.
        def evaluateGroupKey(groupKey: Key): NeedTable = {
          val groupKeyTable = jValueFromGroupKey(groupKey, fullSchema)

          def map(gid: GroupId): NeedTable = {
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
        val tables: StreamT[M, Table] = StreamT.unfoldM(groupKeys.toList) {
          case k :: ks => evaluateGroupKey(k).map(t => Some((t, ks)))
          case Nil     => Need(None)
        }

        Need(Table(tables flatMap (_.slices), UnknownSize))
      }
    }

    /// Utility Methods ///

    /**
      * Reduce the specified table to obtain the in-memory set of strings representing the vfs paths
      * to be loaded.
      */
    protected def pathsM(table: Table) = {
      table reduce {
        new CReducer[Set[Path]] {
          def reduce(schema: CSchema, range: Range): Set[Path] = {
            schema.columns(JTextT) flatMap {
              case s: StrColumn => range.filter(s.isDefinedAt).map(i => Path(s(i)))
              case _            => Set()
            }
          }
        }
      }
    }

    def fromRValues(values: Stream[RValue], maxSliceSize: Option[Int]): Table = {
      val sliceSize = maxSliceSize.getOrElse(yggConfig.maxSliceSize)

      def makeSlice(data: Stream[RValue]): (Slice, Stream[RValue]) = {
        val (prefix, suffix) = data.splitAt(sliceSize)

        (Slice.fromRValues(prefix), suffix)
      }

      Table(
        StreamT.unfoldM(values) { events =>
          Need {
            (!events.isEmpty) option {
              makeSlice(events.toStream)
            }
          }
        },
        ExactSize(values.length)
      )
    }

    def join(left: Table, right: Table, orderHint: Option[JoinOrder])(leftKeySpec: TransSpec1,
                                                                             rightKeySpec: TransSpec1,
                                                                             joinSpec: TransSpec2): M[JoinOrder -> Table] = {
      val emptySpec = trans.ConstLiteral(CEmptyArray, Leaf(Source))
      for {
        left0  <- left.sort(leftKeySpec)
        right0 <- right.sort(rightKeySpec)
        cogrouped = left0.cogroup(leftKeySpec, rightKeySpec, right0)(emptySpec, emptySpec, trans.WrapArray(joinSpec))
      } yield {
        JoinOrder.KeyOrder -> cogrouped.transform(trans.DerefArrayStatic(Leaf(Source), CPathIndex(0)))
      }
    }

    def cross(left: Table, right: Table, orderHint: Option[CrossOrder])(spec: TransSpec2): M[CrossOrder -> Table] = {
      import CrossOrder._
      Need(orderHint match {
        case Some(CrossRight | CrossRightLeft) =>
          CrossRight -> right.cross(left)(TransSpec2.flip(spec))
        case _ =>
          CrossLeft -> left.cross(right)(spec)
      })
    }
  }

  abstract class ColumnarTable(val slices: NeedSlices, val size: TableSize) extends ygg.table.Table {
    self: Table =>

    type Table = outer.Table

    import SliceTransform._

    def sample(sampleSize: Int, specs: Seq[TransSpec1]): Need[Seq[Table]] = Sampling.sample[Table](self, sampleSize, specs)

    /**
      * Folds over the table to produce a single value (stored in a singleton table).
      */
    def reduce[A](reducer: CReducer[A])(implicit monoid: Monoid[A]): M[A] = {
      def rec(stream: StreamT[M, A], acc: A): M[A] = stream.uncons flatMap {
        case Some((head, tail)) => rec(tail, head |+| acc)
        case None               => Need(acc)
      }
      rec(
        slices map (s => reducer.reduce(new CSchema(s.columns.keySet, s logicalColumns _), 0 until s.size)),
        monoid.zero
      )
    }

    def compact(spec: TransSpec1, definedness: Definedness): Table = {
      val specTransform = SliceTransform.composeSliceTransform(spec)
      val compactTransform = {
        SliceTransform.composeSliceTransform(Leaf(Source)).zip(specTransform) { (s1, s2) =>
          s1.compact(s2, definedness)
        }
      }
      Table(Table.transformStream(compactTransform, slices), size).normalize
    }

    /**
      * Performs a one-pass transformation of the keys and values in the table.
      * If the key transform is not identity, the resulting table will have
      * unknown sort order.
      */
    def transform(spec: TransSpec1): Table = {
      Table(Table.transformStream(composeSliceTransform(spec), slices), this.size)
    }

    def force: NeedTable = {
      def loop(slices: NeedSlices, acc: List[Slice], size: Long): M[List[Slice] -> Long] = slices.uncons flatMap {
        case Some((slice, tail)) if slice.size > 0 => loop(tail, slice.materialized :: acc, size + slice.size)
        case Some((_, tail))                       => loop(tail, acc, size)
        case None                                  => Need(acc.reverse -> size)
      }
      val former = new (Id.Id ~> M) { def apply[A](a: Id.Id[A]): M[A] = Need(a) }
      loop(slices, Nil, 0L) map {
        case (stream, size) =>
          Table(StreamT.fromIterable(stream).trans(former), ExactSize(size))
      }
    }

    def paged(limit: Int): Table = {
      val slices2 = slices flatMap { slice =>
        StreamT.unfoldM(0) { idx =>
          val back =
            if (idx >= slice.size)
              None
            else
              Some((slice.takeRange(idx, limit), idx + limit))

          Need(back)
        }
      }

      Table(slices2, size)
    }

    def concat(t2: Table): Table = {
      val resultSize   = TableSize(size.maxSize + t2.size.maxSize)
      val resultSlices = slices ++ t2.slices
      Table(resultSlices, resultSize)
    }

    /**
      * Zips two tables together in their current sorted order.
      * If the tables are not normalized first and thus have different slices sizes,
      * then since the zipping is done per slice, this can produce a result that is
      * different than if the tables were normalized.
      */
    def zip(t2: Table): NeedTable = {
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

    def toArray[A](implicit tpe: CValueType[A]): Table = {
      val slices2: NeedSlices = slices map { _.toArray[A] }
      Table(slices2, size)
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

      Table(StreamT(step(0, Nil, slices)), size)
    }

    /**
      * Cogroups this table with another table, using equality on the specified
      * transformation on rows of the table.
      */
    def cogroup(leftKey: TransSpec1, rightKey: TransSpec1, that: Table)(leftResultTrans: TransSpec1,
                                                                        rightResultTrans: TransSpec1,
                                                                        bothResultTrans: TransSpec2): Table = {

      //println("Cogrouping with respect to\nleftKey: " + leftKey + "\nrightKey: " + rightKey)
      class IndexBuffers(lInitialSize: Int, rInitialSize: Int) {
        val lbuf   = new ArrayIntList(lInitialSize)
        val rbuf   = new ArrayIntList(rInitialSize)
        val leqbuf = new ArrayIntList(lInitialSize max rInitialSize)
        val reqbuf = new ArrayIntList(lInitialSize max rInitialSize)

        @inline def advanceLeft(lpos: Int): Unit = {
          lbuf.add(lpos)
          rbuf.add(-1)
          leqbuf.add(-1)
          reqbuf.add(-1)
          ()
        }

        @inline def advanceRight(rpos: Int): Unit = {
          lbuf.add(-1)
          rbuf.add(rpos)
          leqbuf.add(-1)
          reqbuf.add(-1)
          ()
        }

        @inline def advanceBoth(lpos: Int, rpos: Int): Unit = {
          //println("advanceBoth: lpos = %d, rpos = %d" format (lpos, rpos))
          lbuf.add(-1)
          rbuf.add(-1)
          leqbuf.add(lpos)
          reqbuf.add(rpos)
          ()
        }

        def cogrouped[LR, RR, BR](lslice: Slice,
                                  rslice: Slice,
                                  leftTransform: SliceTransform1[LR],
                                  rightTransform: SliceTransform1[RR],
                                  bothTransform: SliceTransform2[BR]): M[(Slice, LR, RR, BR)] = {

          val remappedLeft  = lslice.remap(lbuf)
          val remappedRight = rslice.remap(rbuf)

          val remappedLeq = lslice.remap(leqbuf)
          val remappedReq = rslice.remap(reqbuf)

          for {
            pairL <- leftTransform(remappedLeft)
            pairR <- rightTransform(remappedRight)
            pairB <- bothTransform(remappedLeq, remappedReq)
          } yield {
            val (ls0, lx) = pairL
            val (rs0, rx) = pairR
            val (bs0, bx) = pairB

            scala.Predef.assert(lx.size == rx.size && rx.size == bx.size)
            val resultSlice = lx zip rx zip bx

            (resultSlice, ls0, rs0, bs0)
          }
        }

        override def toString = {
          "left: " + lbuf.toArray.mkString("[", ",", "]") + "\n" +
            "right: " + rbuf.toArray.mkString("[", ",", "]") + "\n" +
            "both: " + (leqbuf.toArray zip reqbuf.toArray).mkString("[", ",", "]")
        }
      }

      case class SlicePosition[K](sliceId: SliceId,
                                  /** The position in the current slice. This will only be nonzero when the slice has been appended
                                    * to as a result of a cartesian crossing the slice boundary */
                                  pos: Int,
                                  /** Present if not in a final right or left run. A pair of a key slice that is parallel to the
                                    * current data slice, and the value that is needed as input to sltk or srtk to produce the next key. */
                                  keyState: K,
                                  key: Slice,
                                  /** The current slice to be operated upon. */
                                  data: Slice,
                                  /** The remainder of the stream to be operated upon. */
                                  tail: NeedSlices)

      sealed trait NextStep[A, B] extends Product with Serializable
      final case class SplitLeft[A, B](lpos: Int)  extends NextStep[A, B]
      final case class SplitRight[A, B](rpos: Int) extends NextStep[A, B]
      final case class NextCartesianLeft[A, B](left: SlicePosition[A],
                                         right: SlicePosition[B],
                                         rightStart: Option[SlicePosition[B]],
                                         rightEnd: Option[SlicePosition[B]])
          extends NextStep[A, B]
      final case class NextCartesianRight[A, B](left: SlicePosition[A],
                                          right: SlicePosition[B],
                                          rightStart: Option[SlicePosition[B]],
                                          rightEnd: Option[SlicePosition[B]])
          extends NextStep[A, B]
      final case class SkipRight[A, B](left: SlicePosition[A], rightEnd: SlicePosition[B])                                  extends NextStep[A, B]
      final case class RestartRight[A, B](left: SlicePosition[A], rightStart: SlicePosition[B], rightEnd: SlicePosition[B]) extends NextStep[A, B]
      def cogroup0[LK, RK, LR, RR, BR](stlk: SliceTransform1[LK],
                                       strk: SliceTransform1[RK],
                                       stlr: SliceTransform1[LR],
                                       strr: SliceTransform1[RR],
                                       stbr: SliceTransform2[BR]) = {

        sealed trait CogroupState extends Product with Serializable
        final case class EndLeft(lr: LR, lhead: Slice, ltail: NeedSlices) extends CogroupState
        final case class Cogroup(lr: LR,
                           rr: RR,
                           br: BR,
                           left: SlicePosition[LK],
                           right: SlicePosition[RK],
                           rightStart: Option[SlicePosition[RK]],
                           rightEnd: Option[SlicePosition[RK]])
            extends CogroupState
        final case class EndRight(rr: RR, rhead: Slice, rtail: NeedSlices) extends CogroupState
        case object CogroupDone                                            extends CogroupState

        // step is the continuation function fed to uncons. It is called once for each emitted slice
        def step(state: CogroupState): M[Option[Slice -> CogroupState]] = {
          // step0 is the inner monadic recursion needed to cross slice boundaries within the emission of a slice
          def step0(lr: LR,
                    rr: RR,
                    br: BR,
                    leftPosition: SlicePosition[LK],
                    rightPosition: SlicePosition[RK],
                    rightStart0: Option[SlicePosition[RK]],
                    rightEnd0: Option[SlicePosition[RK]]): Need[Option[Slice -> CogroupState]] = {

            val ibufs = new IndexBuffers(leftPosition.key.size, rightPosition.key.size)
            val SlicePosition(lSliceId, lpos0, lkstate, lkey, lhead, ltail) = leftPosition
            val SlicePosition(rSliceId, rpos0, rkstate, rkey, rhead, rtail) = rightPosition

            val comparator = Slice.rowComparatorFor(lkey, rkey) {
              // since we've used the key transforms, and since transforms are contracturally
              // forbidden from changing slice size, we can just use all
              _.columns.keys map (_.selector)
            }

            // the inner tight loop; this will recur while we're within the bounds of
            // a pair of slices. Any operation that must cross slice boundaries
            // must exit this inner loop and recur through the outer monadic loop
            // xrstart is an int with sentinel value for effieiency, but is Option at the slice level.
            @inline
            @tailrec
            def buildRemappings(lpos: Int,
                                rpos: Int,
                                rightStart: Option[SlicePosition[RK]],
                                rightEnd: Option[SlicePosition[RK]],
                                endRight: Boolean): NextStep[LK, RK] = {
              // println("lpos = %d, rpos = %d, rightStart = %s, rightEnd = %s, endRight = %s" format (lpos, rpos, rightStart, rightEnd, endRight))
              // println("Left key: " + lkey.toJson(lpos))
              // println("Right key: " + rkey.toJson(rpos))
              // println("Left data: " + lhead.toJson(lpos))
              // println("Right data: " + rhead.toJson(rpos))

              rightStart match {
                case Some(resetMarker @ SlicePosition(rightStartSliceId, rightStartPos, _, rightStartSlice, _, _)) =>
                  // We're currently in a cartesian.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT if rightStartSliceId == rSliceId =>
                        buildRemappings(lpos + 1, rightStartPos, rightStart, Some(rightPosition.copy(pos = rpos)), endRight)
                      case LT =>
                        // Transition to emit the current slice and reset the right side, carry rightPosition through
                        RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                      case GT =>
                        // catch input-out-of-order errors early
                        rightEnd match {
                          case None =>
                            abort(
                              "Inputs are not sorted; value on the left exceeded value on the right at the end of equal span. lpos = %d, rpos = %d"
                                .format(lpos, rpos))

                          case Some(SlicePosition(endSliceId, endPos, _, endSlice, _, _)) if endSliceId == rSliceId =>
                            buildRemappings(lpos, endPos, None, None, endRight)

                          case Some(rend @ SlicePosition(endSliceId, _, _, _, _, _)) =>
                            // Step out of buildRemappings so that we can restart with the current rightEnd
                            SkipRight(leftPosition.copy(pos = lpos), rend)
                        }
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, rightStart, rightEnd, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    if (endRight) {
                      RestartRight(leftPosition.copy(pos = lpos + 1), resetMarker, rightPosition.copy(pos = rpos))
                    } else {
                      // right slice is exhausted, so we need to emit that slice from the right tail
                      // then continue in the cartesian
                      NextCartesianRight(leftPosition.copy(pos = lpos), rightPosition.copy(pos = rpos), rightStart, rightEnd)
                    }
                  } else if (rpos < rhead.size) {
                    // left slice is exhausted, so we need to emit that slice from the left tail
                    // then continue in the cartesian
                    NextCartesianLeft(leftPosition, rightPosition.copy(pos = rpos), rightStart, rightEnd)
                  } else {
                    abort("This state should be unreachable, since we only increment one side at a time.")
                  }

                case None =>
                  // not currently in a cartesian, hence we can simply proceed.
                  if (lpos < lhead.size && rpos < rhead.size) {
                    comparator.compare(lpos, rpos) match {
                      case LT =>
                        ibufs.advanceLeft(lpos)
                        buildRemappings(lpos + 1, rpos, None, None, endRight)
                      case GT =>
                        ibufs.advanceRight(rpos)
                        buildRemappings(lpos, rpos + 1, None, None, endRight)
                      case EQ =>
                        ibufs.advanceBoth(lpos, rpos)
                        buildRemappings(lpos, rpos + 1, Some(rightPosition.copy(pos = rpos)), None, endRight)
                    }
                  } else if (lpos < lhead.size) {
                    // right side is exhausted, so we should just split the left and emit
                    SplitLeft(lpos)
                  } else if (rpos < rhead.size) {
                    // left side is exhausted, so we should just split the right and emit
                    SplitRight(rpos)
                  } else {
                    abort("This state should be unreachable, since we only increment one side at a time.")
                  }
              }
            }

            def continue(nextStep: NextStep[LK, RK]): M[Option[Slice -> CogroupState]] = nextStep match {
              case SplitLeft(lpos) =>
                val (lpref, lsuf) = lhead.split(lpos)
                val (_, lksuf)    = lkey.split(lpos)
                ibufs.cogrouped(lpref, rhead, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                  case (completeSlice, lr0, rr0, br0) => {
                    rtail.uncons flatMap {
                      case Some((nextRightHead, nextRightTail)) =>
                        strk.f(rkstate, nextRightHead) map {
                          case (rkstate0, rkey0) => {
                            val nextState = Cogroup(
                              lr0,
                              rr0,
                              br0,
                              SlicePosition(lSliceId, 0, lkstate, lksuf, lsuf, ltail),
                              SlicePosition(rSliceId + 1, 0, rkstate0, rkey0, nextRightHead, nextRightTail),
                              None,
                              None)

                            Some(completeSlice -> nextState)
                          }
                        }

                      case None =>
                        val nextState = EndLeft(lr0, lsuf, ltail)
                        Need(Some(completeSlice -> nextState))
                    }
                  }
                }

              case SplitRight(rpos) =>
                val (rpref, rsuf) = rhead.split(rpos)
                val (_, rksuf)    = rkey.split(rpos)

                ibufs.cogrouped(lhead, rpref, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                  case (completeSlice, lr0, rr0, br0) => {
                    ltail.uncons flatMap {
                      case Some((nextLeftHead, nextLeftTail)) =>
                        stlk.f(lkstate, nextLeftHead) map {
                          case (lkstate0, lkey0) => {
                            val nextState = Cogroup(
                              lr0,
                              rr0,
                              br0,
                              SlicePosition(lSliceId + 1, 0, lkstate0, lkey0, nextLeftHead, nextLeftTail),
                              SlicePosition(rSliceId, 0, rkstate, rksuf, rsuf, rtail),
                              None,
                              None)

                            Some(completeSlice -> nextState)
                          }
                        }

                      case None =>
                        val nextState = EndRight(rr0, rsuf, rtail)
                        Need(Some(completeSlice -> nextState))
                    }
                  }
                }

              case NextCartesianLeft(left, right, rightStart, rightEnd) =>
                left.tail.uncons flatMap {
                  case Some((nextLeftHead, nextLeftTail)) =>
                    ibufs
                      .cogrouped(left.data, right.data, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                      case (completeSlice, lr0, rr0, br0) => {
                        stlk.f(lkstate, nextLeftHead) map {
                          case (lkstate0, lkey0) => {
                            val nextState =
                              Cogroup(lr0, rr0, br0, SlicePosition(lSliceId + 1, 0, lkstate0, lkey0, nextLeftHead, nextLeftTail), right, rightStart, rightEnd)

                            Some(completeSlice -> nextState)
                          }
                        }
                      }
                    }

                  case None =>
                    (rightStart, rightEnd) match {
                      case (Some(_), Some(end)) =>
                        val (rpref, rsuf) = end.data.split(end.pos)

                        ibufs
                          .cogrouped(left.data, rpref, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) map {
                          case (completeSlice, lr0, rr0, br0) => {
                            val nextState = EndRight(rr0, rsuf, end.tail)
                            Some(completeSlice -> nextState)
                          }
                        }

                      case _ =>
                        ibufs.cogrouped(
                          left.data,
                          right.data,
                          SliceTransform1[LR](lr, stlr.f),
                          SliceTransform1[RR](rr, strr.f),
                          SliceTransform2[BR](br, stbr.f)) map {
                          case (completeSlice, lr0, rr0, br0) =>
                            Some(completeSlice -> CogroupDone)
                        }
                    }
                }

              case NextCartesianRight(left, right, rightStart, rightEnd) =>
                right.tail.uncons flatMap {
                  case Some((nextRightHead, nextRightTail)) =>
                    ibufs
                      .cogrouped(left.data, right.data, SliceTransform1[LR](lr, stlr.f), SliceTransform1[RR](rr, strr.f), SliceTransform2[BR](br, stbr.f)) flatMap {
                      case (completeSlice, lr0, rr0, br0) => {
                        strk.f(rkstate, nextRightHead) map {
                          case (rkstate0, rkey0) => {
                            val nextState =
                              Cogroup(lr0, rr0, br0, left, SlicePosition(rSliceId + 1, 0, rkstate0, rkey0, nextRightHead, nextRightTail), rightStart, rightEnd)

                            Some(completeSlice -> nextState)
                          }
                        }
                      }
                    }

                  case None =>
                    continue(buildRemappings(left.pos, right.pos, rightStart, rightEnd, true))
                }

              case SkipRight(left, rightEnd) =>
                step0(lr, rr, br, left, rightEnd, None, None)

              case RestartRight(left, rightStart, rightEnd) =>
                ibufs.cogrouped(
                  left.data,
                  rightPosition.data,
                  SliceTransform1[LR](lr, stlr.f),
                  SliceTransform1[RR](rr, strr.f),
                  SliceTransform2[BR](br, stbr.f)) map {
                  case (completeSlice, lr0, rr0, br0) => {
                    val nextState = Cogroup(lr0, rr0, br0, left, rightStart, Some(rightStart), Some(rightEnd))

                    Some(completeSlice -> nextState)
                  }
                }
            }

            continue(buildRemappings(lpos0, rpos0, rightStart0, rightEnd0, false))
          } // end of step0

          state match {
            case EndLeft(lr, data, tail) =>
              stlr.f(lr, data) flatMap {
                case (lr0, leftResult) => {
                  tail.uncons map { unconsed =>
                    Some(leftResult -> (unconsed map { case (nhead, ntail) => EndLeft(lr0, nhead, ntail) } getOrElse CogroupDone))
                  }
                }
              }

            case Cogroup(lr, rr, br, left, right, rightReset, rightEnd) =>
              step0(lr, rr, br, left, right, rightReset, rightEnd)

            case EndRight(rr, data, tail) =>
              strr.f(rr, data) flatMap {
                case (rr0, rightResult) => {
                  tail.uncons map { unconsed =>
                    Some(rightResult -> (unconsed map { case (nhead, ntail) => EndRight(rr0, nhead, ntail) } getOrElse CogroupDone))
                  }
                }
              }

            case CogroupDone => Need(None)
          }
        } // end of step

        val initialState = for {
          // We have to compact both sides to avoid any rows for which the key is completely undefined
          leftUnconsed  <- self.compact(leftKey).slices.uncons
          rightUnconsed <- that.compact(rightKey).slices.uncons

          back <- {
            val cogroup = for {
              (leftHead, leftTail)   <- leftUnconsed
              (rightHead, rightTail) <- rightUnconsed
            } yield {
              for {
                pairL <- stlk(leftHead)
                pairR <- strk(rightHead)
              } yield {
                val (lkstate, lkey) = pairL
                val (rkstate, rkey) = pairR

                Cogroup(
                  stlr.initial,
                  strr.initial,
                  stbr.initial,
                  SlicePosition(SliceId(0), 0, lkstate, lkey, leftHead, leftTail),
                  SlicePosition(SliceId(0), 0, rkstate, rkey, rightHead, rightTail),
                  None,
                  None
                )
              }
            }

            val optM = cogroup orElse {
              leftUnconsed map {
                case (head, tail) => EndLeft(stlr.initial, head, tail)
              } map (Need(_))
            } orElse {
              rightUnconsed map {
                case (head, tail) => EndRight(strr.initial, head, tail)
              } map (Need(_))
            }

            optM map { m =>
              m map { Some(_) }
            } getOrElse {
              Need(None)
            }
          }
        } yield back

        Table(StreamT.wrapEffect(initialState map { state =>
          StreamT.unfoldM[M, Slice, CogroupState](state getOrElse CogroupDone)(step)
        }), UnknownSize)
      }

      cogroup0(
        composeSliceTransform(leftKey),
        composeSliceTransform(rightKey),
        composeSliceTransform(leftResultTrans),
        composeSliceTransform(rightResultTrans),
        composeSliceTransform2(bothResultTrans))
    }

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

          StreamT.unfoldM(CrossState(a0, 0, right))(step _)
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

      if (sizeCheck getOrElse true) {
        Table(StreamT(cross0(composeSliceTransform2(spec)) map { tail =>
          StreamT.Skip(tail)
        }), newSize)
      } else {
        throw EnormousCartesianException(this.size, that.size)
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

        val slices0 = StreamT.wrapEffect(this.sort(spec) map { sorted =>
          stream((id.initial, filter.initial), sorted.slices)
        })

        Table(slices0, EstimateSize(0L, size.maxSize))
      }

      distinct0(SliceTransform.identity(None: Option[Slice]), composeSliceTransform(spec))
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

      Table(StreamT.wrapEffect(loop(slices, 0)), newSize)
    }

    /**
      * In order to call partitionMerge, the table must be sorted according to
      * the values specified by the partitionBy transspec.
      */
    def partitionMerge(partitionBy: TransSpec1)(f: Table => NeedTable): NeedTable = {
      // Find the first element that compares LT
      @tailrec def findEnd(compare: Int => Ordering, imin: Int, imax: Int): Int = {
        val minOrd = compare(imin)
        if (minOrd eq EQ) {
          val maxOrd = compare(imax)
          if (maxOrd eq EQ) {
            imax + 1
          } else if (maxOrd eq LT) {
            val imid   = imin + ((imax - imin) / 2)
            val midOrd = compare(imid)
            if (midOrd eq LT) {
              findEnd(compare, imin, imid - 1)
            } else if (midOrd eq EQ) {
              findEnd(compare, imid, imax - 1)
            } else {
              abort("Inputs to partitionMerge not sorted.")
            }
          } else {
            abort("Inputs to partitionMerge not sorted.")
          }
        } else if ((minOrd eq LT) && (compare(imax) eq LT)) {
          imin
        } else {
          abort("Inputs to partitionMerge not sorted.")
        }
      }

      def subTable(comparatorGen: Slice => (Int => Ordering), slices: NeedSlices): NeedTable = {
        def subTable0(slices: NeedSlices, subSlices: NeedSlices, size: Int): NeedTable = {
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

    def normalize: Table = Table(slices.filter(!_.isEmpty), size)

    def schemas: Need[Set[JType]] = {

      // Returns true iff masks contains an array equivalent to mask.
      def contains(masks: List[RawBitSet], mask: Array[Int]): Boolean = {

        @tailrec
        def equal(x: Array[Int], y: Array[Int], i: Int): Boolean =
          if (i >= x.length) {
            true
          } else if (x(i) != y(i)) {
            false
          } else {
            equal(x, y, i + 1)
          }

        @tailrec
        def loop(xs: List[RawBitSet], y: Array[Int]): Boolean = xs match {
          case x :: xs if x.length == y.length && equal(x.bits, y, 0) => true
          case _ :: xs                                                => loop(xs, y)
          case Nil                                                    => false
        }

        loop(masks, mask)
      }

      def isZero(x: Array[Int]): Boolean = {
        @tailrec def loop(i: Int): Boolean =
          if (i < 0) {
            true
          } else if (x(i) != 0) {
            false
          } else {
            loop(i - 1)
          }

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
      def collectSchemas(schemas: Set[JType], slices: NeedSlices): M[Set[JType]] = {
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
}

final case class EnormousCartesianException(left: TableSize, right: TableSize) extends RuntimeException {
  override def getMessage =
    "cannot evaluate cartesian of sets with size %s and %s".format(left, right)
}
