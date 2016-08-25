package ygg.table

import scalaz._, Scalaz._, Ordering._
import ygg._, common._, data._, json._, trans._
import org.mapdb._
import JDBM._

trait BlockStoreColumnarTableModule extends ColumnarTableModule {
  type TableCompanion <: BlockStoreColumnarTableCompanion

  protected class MergeEngine[KeyType, BlockData <: BlockProjectionData[KeyType]] {
    case class CellState(index: Int, maxKey: KeyType, slice0: Slice, succf: KeyType => M[Option[BlockData]], remap: Array[Int], position: Int) {
      def toCell = {
        new Cell(index, maxKey, slice0)(succf, remap.clone, position)
      }
    }

    object CellState {
      def apply(index: Int, maxKey: KeyType, slice0: Slice, succf: KeyType => M[Option[BlockData]]) = {
        val remap = new Array[Int](slice0.size)
        new CellState(index, maxKey, slice0, succf, remap, 0)
      }
    }

    /**
      * A wrapper for a slice, and the function required to get the subsequent
      * block of data.
      */
    case class Cell private[MergeEngine] (index: Int, maxKey: KeyType, slice0: Slice)(succf: KeyType => M[Option[BlockData]],
                                                                                      remap: Array[Int],
                                                                                      var position: Int) {
      def advance(i: Int): Boolean = {
        if (position < slice0.size) {
          remap(position) = i
          position += 1
        }

        position < slice0.size
      }

      def slice = {
        slice0.sparsen(remap, if (position > 0) remap(position - 1) + 1 else 0)
      }

      def succ: M[Option[CellState]] = {
        for (blockOpt <- succf(maxKey)) yield {
          blockOpt map { block =>
            CellState(index, block.maxKey, block.data, succf)
          }
        }
      }

      def split: (Slice, CellState) = {
        val (finished, continuing) = slice0.split(position)
        val nextState              = CellState(index, maxKey, continuing, succf)
        (if (position == 0) finished else finished.sparsen(remap, remap(position - 1) + 1), nextState)
      }

      // Freeze the state of this cell. Used to ensure restartability from any point in a stream of slices derived
      // from mergeProjections.
      def state: CellState = {
        val remap0 = new Array[Int](slice0.size)
        System.arraycopy(remap, 0, remap0, 0, slice0.size)
        new CellState(index, maxKey, slice0, succf, remap0, position)
      }
    }

    sealed trait CellMatrix { self =>
      def cells: Iterable[Cell]
      def compare(cl: Cell, cr: Cell): Ordering

      lazy val ordering = Ord.order[Cell](compare(_, _))
    }

    object CellMatrix {
      def apply(initialCells: Vector[Cell])(keyf: Slice => Iterable[CPath]): CellMatrix = {
        val size = if (initialCells.isEmpty) 0 else initialCells.map(_.index).max + 1

        type ComparatorMatrix = Array[Array[RowComparator]]
        def fillMatrix(initialCells: Vector[Cell]): ComparatorMatrix = {
          val comparatorMatrix = Array.ofDim[RowComparator](size, size)

          for (Cell(i, _, s) <- initialCells; Cell(i0, _, s0) <- initialCells if i != i0) {
            comparatorMatrix(i)(i0) = Slice.rowComparatorFor(s, s0)(keyf)
          }

          comparatorMatrix
        }

        new CellMatrix {
          private[this] val allCells: scmMap[Int, Cell] = initialCells.map(c => (c.index, c))(collection.breakOut)
          private[this] val comparatorMatrix            = fillMatrix(initialCells)

          def cells = allCells.values

          def compare(cl: Cell, cr: Cell): Ordering = {
            comparatorMatrix(cl.index)(cr.index).compare(cl.position, cr.position)
          }
        }
      }
    }

    def mergeProjections(inputSortOrder: DesiredSortOrder, cellStates: Stream[CellState])(keyf: Slice => Iterable[CPath]): StreamT[M, Slice] = {

      // dequeues all equal elements from the head of the queue
      @inline
      @tailrec
      def dequeueEqual(
          queue: scmPriQueue[Cell],
          cellMatrix: CellMatrix,
          cells: List[Cell]
      ): List[Cell] =
        if (queue.isEmpty) {
          cells
        } else if (cells.isEmpty || cellMatrix.compare(queue.head, cells.head) == EQ) {
          dequeueEqual(queue, cellMatrix, queue.dequeue() :: cells)
        } else {
          cells
        }

      // consume as many records as possible
      @inline
      @tailrec
      def consumeToBoundary(
          queue: scmPriQueue[Cell],
          cellMatrix: CellMatrix,
          idx: Int
      ): (Int, List[Cell]) = {
        val cellBlock = dequeueEqual(queue, cellMatrix, Nil)

        if (cellBlock.isEmpty) {
          // At the end of data, since this will only occur if nothing
          // remains in the priority queue
          (idx, Nil)
        } else {
          val (continuing, expired) = cellBlock partition { _.advance(idx) }
          queue.enqueue(continuing: _*)

          if (expired.isEmpty)
            consumeToBoundary(queue, cellMatrix, idx + 1)
          else
            (idx + 1, expired)
        }
      }

      StreamT.unfoldM[M, Slice, Stream[CellState]](cellStates) { cellStates =>
        val cells: Vector[Cell] = cellStates.map(_.toCell)(collection.breakOut)

        // TODO: We should not recompute all of the row comparators every time,
        // since all but one will still be valid and usable. However, getting
        // to this requires more significant rework than can be undertaken
        // right now.
        val cellMatrix = CellMatrix(cells)(keyf)

        val queue = mutableQueue(cells.toSeq: _*)(
          if (inputSortOrder.isAscending)
            cellMatrix.ordering.reverseOrder
          else
            cellMatrix.ordering
        )


        val (finishedSize, expired) = consumeToBoundary(queue, cellMatrix, 0)
        if (expired.isEmpty) {
          Need(None)
        } else {
          val completeSlices = expired.map(_.slice)

          val (prefixes, suffixes) = queue.dequeueAll.map(_.split).unzip

          val emission = Slice(finishedSize, {
            (completeSlices.flatMap(_.columns) ++ prefixes.flatMap(_.columns)).groupBy(_._1).map {
              case (ref, columns) => {
                val cp: ColumnRef -> Column = if (columns.size == 1) {
                  columns.head
                } else {
                  (ref, ArraySetColumn(ref.ctype, columns.map(_._2).toArray))
                }
                cp
              }
            }
          })

          val successorStatesM = expired.map(_.succ).sequence.map(_.toStream.collect({ case Some(cs) => cs }))

          successorStatesM map { successorStates =>
            Some((emission, successorStates ++ suffixes))
          }
        }
      }
    }
  }

  trait BlockStoreColumnarTableCompanion extends ColumnarTableCompanion {
    import SliceTransform._

    type SortBlockData = BlockProjectionData[Bytes]

    lazy val sortMergeEngine = new MergeEngine[Bytes, SortBlockData] {}

    case class JDBMState(prefix: String, fdb: Option[File -> DB], indices: IndexMap, insertCount: Long) {
      def commit() = fdb foreach { _._2.commit() }

      def closed(): JDBMState = fdb match {
        case Some((f, db)) =>
          db.close()
          JDBMState(prefix, None, indices, insertCount)
        case None => this
      }

      def opened(): (File, DB, JDBMState) = fdb match {
        case Some((f, db)) => (f, db, this)
        case None          =>
          // Open a JDBM3 DB for use in sorting under a temp directory
          val dbFile = new File(newScratchDir(), prefix)
          val db     = DBMaker.fileDB(dbFile.getCanonicalPath).make()
          (dbFile, db, JDBMState(prefix, Some((dbFile, db)), indices, insertCount))
      }
    }
    object JDBMState {
      def empty(prefix: String) = JDBMState(prefix, None, Map(), 0l)
    }

    case class WriteState(jdbmState: JDBMState, valueTrans: SliceTransform1[_], keyTransformsWithIds: List[SliceTransform1[_] -> String])

    object addGlobalIdScanner extends Scanner {
      type A = Long
      val init = 0l
      def scan(a: Long, cols: ColumnMap, range: Range): A -> ColumnMap = {
        val globalIdColumn = new RangeColumn(range) with LongColumn { def apply(row: Int) = a + row }
        (a + range.end + 1, cols + (ColumnRef(CPath(CPathIndex(1)), CLong) -> globalIdColumn))
      }
    }

    def addGlobalId(spec: TransSpec1) = {
      Scan(WrapArray(spec), addGlobalIdScanner)
    }

    def apply(slices: StreamT[M, Slice], size: TableSize): Table = {
      size match {
        case ExactSize(1) => new SingletonTable(slices)
        case _            => new ExternalTable(slices, size)
      }
    }

    def singleton(slice: Slice) = new SingletonTable(singleStreamT(slice))

    def align(sourceL: Table, alignL: TransSpec1, sourceR: Table, alignR: TransSpec1): Need[PairOf[Table]] = {
      import aligns._

      // we need a custom row comparator that ignores the global ID introduced to prevent elimination of
      // duplicate rows in the write to JDBM
      def buildRowComparator(lkey: Slice, rkey: Slice, rauth: Slice): RowComparator = new RowComparator {
        private val mainComparator = Slice.rowComparatorFor(lkey.deref(CPathIndex(0)), rkey.deref(CPathIndex(0))) {
          _.columns.keys map (_.selector)
        }

        private val auxComparator =
          if (rauth == null) null
          else {
            Slice.rowComparatorFor(lkey.deref(CPathIndex(0)), rauth.deref(CPathIndex(0))) {
              _.columns.keys map (_.selector)
            }
          }

        def compare(i1: Int, i2: Int) = {
          if (i2 < 0 && rauth != null) auxComparator.compare(i1, rauth.size + i2) else mainComparator.compare(i1, i2)
        }
      }

      // this method exists only to skolemize A and B
      def writeStreams[A, B](left: StreamT[M, Slice],
                             leftKeyTrans: SliceTransform1[A],
                             right: StreamT[M, Slice],
                             rightKeyTrans: SliceTransform1[B],
                             leftWriteState: JDBMState,
                             rightWriteState: JDBMState): LazyPairOf[Table] = {

        // We will *always* have a lhead and rhead, because if at any point we
        // run out of data, we'll still be hanging on to the last slice on the
        // other side to use as the authority for equality comparisons
        def step(
            state: AlignState,
            lhead: Slice,
            ltail: StreamT[M, Slice],
            stepleq: BitSet,
            rhead: Slice,
            rtail: StreamT[M, Slice],
            stepreq: BitSet,
            lstate: A,
            rstate: B,
            leftWriteState: JDBMState,
            rightWriteState: JDBMState
        ): LazyPairOf[JDBMState] = {

          @tailrec
          def buildFilters(comparator: RowComparator, lidx: Int, lsize: Int, lacc: BitSet, ridx: Int, rsize: Int, racc: BitSet, span: Span): NextStep = {
            //println((lidx, ridx, span))

            // todo: This is optimized for sparse alignments; if you get into an alignment
            // where every pair is distinct and equal, you'll do 2*n comparisons.
            // This should instead be optimized for dense alignments, using an algorithm that
            // advances both sides after an equal, then backtracks on inequality
            if (span eq LeftSpan) {
              // We don't need to compare the index on the right, since it will be left unchanged
              // throughout the time that we're advancing left, and even if it's beyond the end of
              // input we can use the next-to-last element for comparison

              if (lidx < lsize) {
                comparator.compare(lidx, ridx - 1) match {
                  case EQ =>
                    //println("Found equal on left.")
                    buildFilters(comparator, lidx + 1, lsize, lacc + lidx, ridx, rsize, racc, LeftSpan)
                  case LT =>
                    abort("Inputs to align are not correctly sorted.")
                  case GT =>
                    buildFilters(comparator, lidx, lsize, lacc, ridx, rsize, racc, NoSpan)
                }
              } else {
                // left is exhausted in the midst of a span
                //println("Left exhausted in the middle of a span.")
                MoreLeft(LeftSpan, lacc, ridx, racc)
              }
            } else {
              if (lidx < lsize && ridx < rsize) {
                comparator.compare(lidx, ridx) match {
                  case EQ =>
                    //println("Found equal on right.")
                    buildFilters(comparator, lidx, lsize, lacc, ridx + 1, rsize, racc + ridx, RightSpan)
                  case LT =>
                    if (span eq RightSpan) {
                      // drop into left spanning of equal
                      buildFilters(comparator, lidx, lsize, lacc, ridx, rsize, racc, LeftSpan)
                    } else {
                      // advance the left in the not-left-spanning state
                      buildFilters(comparator, lidx + 1, lsize, lacc, ridx, rsize, racc, NoSpan)
                    }
                  case GT =>
                    if (span eq RightSpan) abort("Inputs to align are not correctly sorted")
                    else buildFilters(comparator, lidx, lsize, lacc, ridx + 1, rsize, racc, NoSpan)
                }
              } else if (lidx < lsize) {
                // right is exhausted; span will be RightSpan or NoSpan
                //println("Right exhausted, left is not; asking for more right with " + lacc.mkString("[", ",", "]") + ";" + racc.mkString("[", ",", "]") )
                MoreRight(span, lidx, lacc, racc)
              } else {
                //println("Both sides exhausted, so emitting with " + lacc.mkString("[", ",", "]") + ";" + racc.mkString("[", ",", "]") )
                MoreLeft(NoSpan, lacc, ridx, racc)
              }
            }
          }

          // this is an optimization that uses a preemptory comparison and a binary
          // search to skip over big chunks of (or entire) slices if possible.
          def findEqual(comparator: RowComparator, leftRow: Int, leq: BitSet, rightRow: Int, req: BitSet): NextStep = {
            comparator.compare(leftRow, rightRow) match {
              case EQ =>
                //println("findEqual is equal at %d, %d".format(leftRow, rightRow))
                buildFilters(comparator, leftRow, lhead.size, leq, rightRow, rhead.size, req, NoSpan)

              case LT =>
                val leftIdx = comparator.nextLeftIndex(leftRow + 1, lhead.size - 1, 0)
                //println("found next left index " + leftIdx + " from " + (lhead.size - 1, lhead.size, 0, lhead.size - leftRow - 1))
                if (leftIdx == lhead.size) {
                  MoreLeft(NoSpan, leq, rightRow, req)
                } else {
                  buildFilters(comparator, leftIdx, lhead.size, leq, rightRow, rhead.size, req, NoSpan)
                }

              case GT =>
                val rightIdx = comparator.swap.nextLeftIndex(rightRow + 1, rhead.size - 1, 0)
                //println("found next right index " + rightIdx + " from " + (rhead.size - 1, rhead.size, 0, rhead.size - rightRow - 1))
                if (rightIdx == rhead.size) {
                  MoreRight(NoSpan, leftRow, leq, req)
                } else {
                  // do a binary search to find the indices where the comparison becomse LT or EQ
                  buildFilters(comparator, leftRow, lhead.size, leq, rightIdx, rhead.size, req, NoSpan)
                }
            }
          }

          // This function exists so that we can correctly nandle the situation where the right side is out of data
          // and we need to continue in a span on the left.
          def continue(nextStep: NextStep,
                       comparator: RowComparator,
                       lstate: A,
                       lkey: Slice,
                       rstate: B,
                       rkey: Slice,
                       leftWriteState: JDBMState,
                       rightWriteState: JDBMState): LazyPairOf[JDBMState] = nextStep match {
            case MoreLeft(span, leq, ridx, req) =>
              def next(lbs: JDBMState, rbs: JDBMState): Need[JDBMState -> JDBMState] = ltail.uncons flatMap {
                case Some((lhead0, ltail0)) =>
                  ///println("Continuing on left; not emitting right.")
                  val nextState = (span: @unchecked) match {
                    case NoSpan => FindEqualAdvancingLeft(ridx, rkey)
                    case LeftSpan =>
                      state match {
                        case RunRight(_, _, rauth) => RunLeft(ridx, rkey, rauth)
                        case RunLeft(_, _, rauth)  => RunLeft(ridx, rkey, rauth)
                        case _                     => RunLeft(ridx, rkey, None)
                      }
                  }

                  step(nextState, lhead0, ltail0, new BitSet, rhead, rtail, req, lstate, rstate, lbs, rbs)

                case None =>
                  //println("No more data on left; emitting right based on bitset " + req.toList.mkString("[", ",", "]"))
                  // done on left, and we're not in an equal span on the right (since LeftSpan can only
                  // be emitted if we're not in a right span) so we're entirely done.
                  val remission = req.nonEmpty.option(rhead.mapColumns(cf.filter(0, rhead.size, req)))
                  (remission map { e =>
                    writeAlignedSlices(rkey, e, rbs, "alignRight", SortAscending)
                  } getOrElse rbs.point[M]) map { (lbs, _) }
              }

              //println("Requested more left; emitting left based on bitset " + leq.toList.mkString("[", ",", "]"))
              val lemission = leq.nonEmpty.option(lhead.mapColumns(cf.filter(0, lhead.size, leq)))
              lemission map { e =>
                for {
                  nextLeftWriteState <- writeAlignedSlices(lkey, e, leftWriteState, "alignLeft", SortAscending)
                  resultWriteStates  <- next(nextLeftWriteState, rightWriteState)
                } yield resultWriteStates
              } getOrElse {
                next(leftWriteState, rightWriteState)
              }

            case MoreRight(span, lidx, leq, req) =>
              def next(lbs: JDBMState, rbs: JDBMState): Need[JDBMState -> JDBMState] = rtail.uncons flatMap {
                case Some((rhead0, rtail0)) =>
                  //println("Continuing on right.")
                  val nextState = (span: @unchecked) match {
                    case NoSpan    => FindEqualAdvancingRight(lidx, lkey)
                    case RightSpan => RunRight(lidx, lkey, Some(rkey))
                  }

                  step(nextState, lhead, ltail, leq, rhead0, rtail0, new BitSet, lstate, rstate, lbs, rbs)

                case None =>
                  // no need here to check for LeftSpan by the contract of buildFilters
                  (span: @unchecked) match {
                    case NoSpan =>
                      //println("No more data on right and not in a span; emitting left based on bitset " + leq.toList.mkString("[", ",", "]"))
                      // entirely done; just emit both
                      val lemission = leq.nonEmpty.option(lhead.mapColumns(cf.filter(0, lhead.size, leq)))
                      (lemission map { e =>
                        writeAlignedSlices(lkey, e, lbs, "alignLeft", SortAscending)
                      } getOrElse lbs.point[M]) map { (_, rbs) }

                    case RightSpan =>
                      //println("No more data on right, but in a span so continuing on left.")
                      // if span == RightSpan and no more data exists on the right, we need to continue in buildFilters spanning on the left.
                      val nextState = buildFilters(comparator, lidx, lhead.size, leq, rhead.size, rhead.size, new BitSet, LeftSpan)
                      continue(nextState, comparator, lstate, lkey, rstate, rkey, lbs, rbs)
                  }
              }

              //println("Requested more right; emitting right based on bitset " + req.toList.mkString("[", ",", "]"))
              val remission = req.nonEmpty.option(rhead.mapColumns(cf.filter(0, rhead.size, req)))
              remission map { e =>
                for {
                  nextRightWriteState <- writeAlignedSlices(rkey, e, rightWriteState, "alignRight", SortAscending)
                  resultWriteStates   <- next(leftWriteState, nextRightWriteState)
                } yield resultWriteStates
              } getOrElse {
                next(leftWriteState, rightWriteState)
              }
          }

          //println("state: " + state)
          state match {
            case FindEqualAdvancingRight(leftRow, lkey) =>
              // whenever we drop into buildFilters in this case, we know that we will be neither
              // in a left span nor a right span because we didn't have an equal case at the
              // last iteration.

              rightKeyTrans.f(rstate, rhead) flatMap {
                case (nextB, rkey) => {
                  val comparator = buildRowComparator(lkey, rkey, null)

                  // do some preliminary comparisons to figure out if we even need to look at the current slice
                  val nextState = findEqual(comparator, leftRow, stepleq, 0, stepreq)
                  //println("Next state: " + nextState)
                  continue(nextState, comparator, lstate, lkey, nextB, rkey, leftWriteState, rightWriteState)
                }
              }

            case FindEqualAdvancingLeft(rightRow, rkey) =>
              // whenever we drop into buildFilters in this case, we know that we will be neither
              // in a left span nor a right span because we didn't have an equal case at the
              // last iteration.

              leftKeyTrans.f(lstate, lhead) flatMap {
                case (nextA, lkey) => {
                  val comparator = buildRowComparator(lkey, rkey, null)

                  // do some preliminary comparisons to figure out if we even need to look at the current slice
                  val nextState = findEqual(comparator, 0, stepleq, rightRow, stepreq)
                  continue(nextState, comparator, nextA, lkey, rstate, rkey, leftWriteState, rightWriteState)
                }
              }

            case RunRight(leftRow, lkey, rauth) =>
              rightKeyTrans.f(rstate, rhead) flatMap {
                case (nextB, rkey) => {
                  val comparator = buildRowComparator(lkey, rkey, rauth.orNull)

                  val nextState = buildFilters(comparator, leftRow, lhead.size, stepleq, 0, rhead.size, new BitSet, RightSpan)
                  continue(nextState, comparator, lstate, lkey, nextB, rkey, leftWriteState, rightWriteState)
                }
              }

            case RunLeft(rightRow, rkey, rauth) =>
              leftKeyTrans.f(lstate, lhead) flatMap {
                case (nextA, lkey) => {
                  val comparator = buildRowComparator(lkey, rkey, rauth.orNull)

                  val nextState = buildFilters(comparator, 0, lhead.size, new BitSet, rightRow, rhead.size, stepreq, LeftSpan)
                  continue(nextState, comparator, nextA, lkey, rstate, rkey, leftWriteState, rightWriteState)
                }
              }
          }
        }

        left.uncons flatMap {
          case Some((lhead, ltail)) =>
            right.uncons.flatMap {
              case Some((rhead, rtail)) =>
                //println("Got data from both left and right.")
                //println("initial left: \n" + lhead + "\n\n")
                //println("initial right: \n" + rhead + "\n\n")
                val stepResult = leftKeyTrans(lhead) flatMap {
                  case (lstate, lkey) => {
                    step(
                      FindEqualAdvancingRight(0, lkey),
                      lhead,
                      ltail,
                      new BitSet,
                      rhead,
                      rtail,
                      new BitSet,
                      lstate,
                      rightKeyTrans.initial,
                      leftWriteState,
                      rightWriteState)
                  }
                }

                for {
                  writeStates <- stepResult
                } yield {
                  val (leftState, rightState) = writeStates
                  val closedLeftState         = leftState.closed()
                  val closedRightState        = rightState.closed()
                  (
                    loadTable(sortMergeEngine, closedLeftState.indices, SortAscending),
                    loadTable(sortMergeEngine, closedRightState.indices, SortAscending)
                  )
                }

              case None =>
                //println("uncons right returned none")
                (Table.empty, Table.empty).point[M]
            }

          case None =>
            //println("uncons left returned none")
            (Table.empty, Table.empty).point[M]
        }
      }

      // We need some id that can be used to memoize then load table for each side.
      val initState = JDBMState.empty("alignSpace")

      writeStreams(
        reduceSlices(sourceL.slices),
        composeSliceTransform(addGlobalId(alignL)),
        reduceSlices(sourceR.slices),
        composeSliceTransform(addGlobalId(alignR)),
        initState,
        initState)
    }

    /**
      * Passes over all slices and returns a new slices that is the concatenation
      * of all the slices. At some point this should lazily chunk the slices into
      * fixed sizes so that we can individually sort/merge.
      */
    protected def reduceSlices(slices: StreamT[M, Slice]): StreamT[M, Slice] = {
      def rec(ss: List[Slice], slices: StreamT[M, Slice]): StreamT[M, Slice] = {
        StreamT[M, Slice](slices.uncons map {
          case Some((head, tail)) => StreamT.Skip(rec(head :: ss, tail))
          case None if ss.isEmpty => StreamT.Done
          case None               => StreamT.Yield(Slice.concat(ss.reverse), emptyStreamT())
        })
      }

      rec(Nil, slices)
    }

    def writeTables(slices: StreamT[M, Slice],
                    valueTrans: SliceTransform1[_],
                    keyTrans: Seq[SliceTransform1[_]],
                    sortOrder: DesiredSortOrder): M[List[String] -> IndexMap] = {
      def write0(slices: StreamT[M, Slice], state: WriteState): M[List[String] -> IndexMap] = {
        slices.uncons flatMap {
          case Some((slice, tail)) =>
            writeSlice(slice, state, sortOrder) flatMap { write0(tail, _) }

          case None =>
            Need {
              val closedJDBMState = state.jdbmState.closed()
              (state.keyTransformsWithIds map (_._2), closedJDBMState.indices)
            }
        }
      }
      val identifiedKeyTrans = keyTrans.zipWithIndex map { case (kt, i) => kt -> i.toString }
      write0(reduceSlices(slices), WriteState(JDBMState.empty("writeSortedSpace"), valueTrans, identifiedKeyTrans.toList))
    }

    protected def writeSlice(slice: Slice, state: WriteState, sortOrder: DesiredSortOrder, source: String = ""): M[WriteState] = {
      val WriteState(jdbmState, valueTrans, keyTrans) = state

      valueTrans.advance(slice) flatMap {
        case (valueTrans0, vslice) => {
          val (vColumnRefs, vColumns) = vslice.columns.toList.sortBy(_._1).unzip
          val dataRowFormat           = RowFormat.forValues(vColumnRefs)
          val dataColumnEncoder       = dataRowFormat.ColumnEncoder(vColumns)

          def storeTransformed(jdbmState: JDBMState,
                               transforms: List[SliceTransform1[_] -> String],
                               updatedTransforms: List[SliceTransform1[_] -> String]): M[JDBMState -> List[SliceTransform1[_] -> String]] = transforms match {
            case (keyTransform, streamId) :: tail =>
              keyTransform.advance(slice) flatMap {
                case (nextKeyTransform, kslice) =>
                  def thing = (nextKeyTransform -> streamId) :: updatedTransforms

                  kslice.columns.toList.sortBy(_._1).unzip match {
                    case (refs, _) if refs.isEmpty =>
                      Need(jdbmState -> thing)
                    case (refs, _) =>
                      writeRawSlices(kslice, sortOrder, vslice, vColumnRefs, dataColumnEncoder, streamId, jdbmState) flatMap (storeTransformed(_, tail, thing))
                  }
              }

            case Nil =>
              Need((jdbmState, updatedTransforms.reverse))
          }

          storeTransformed(jdbmState, keyTrans, Nil) map {
            case (jdbmState0, keyTrans0) =>
              WriteState(jdbmState0, valueTrans0, keyTrans0)
          }
        }
      }
    }

    protected def writeAlignedSlices(kslice: Slice, vslice: Slice, jdbmState: JDBMState, indexNamePrefix: String, sortOrder: DesiredSortOrder) = {
      val (vColumnRefs, vColumns) = vslice.columns.toList.sortBy(_._1).unzip
      val dataRowFormat           = RowFormat.forValues(vColumnRefs)
      val dataColumnEncoder       = dataRowFormat.ColumnEncoder(vColumns)

      writeRawSlices(kslice, sortOrder, vslice, vColumnRefs, dataColumnEncoder, indexNamePrefix, jdbmState)
    }

    protected def writeRawSlices(kslice: Slice,
                                 sortOrder: DesiredSortOrder,
                                 vslice: Slice,
                                 vrefs: List[ColumnRef],
                                 vEncoder: ColumnEncoder,
                                 indexNamePrefix: String,
                                 jdbmState: JDBMState): M[JDBMState] = Need {
      // Iterate over the slice, storing each row
      // FIXME: Determine whether undefined sort keys are valid
      def storeRows(kslice: Slice, vslice: Slice, keyRowFormat: RowFormat, vEncoder: ColumnEncoder, storage: IndexStore, insertCount: Long): Long = {

        val keyColumns = kslice.columns.toList.sortBy(_._1).map(_._2)
        val kEncoder   = keyRowFormat.ColumnEncoder(keyColumns)

        @tailrec def storeRow(row: Int, insertCount: Long): Long = {
          if (row < vslice.size) {
            if (vslice.isDefinedAt(row) && kslice.isDefinedAt(row)) {
              storage.put(kEncoder.encodeFromRow(row), vEncoder.encodeFromRow(row))

              if (insertCount % jdbmCommitInterval == 0 && insertCount > 0) jdbmState.commit()
              storeRow(row + 1, insertCount + 1)
            } else {
              storeRow(row + 1, insertCount)
            }
          } else {
            insertCount
          }
        }

        storeRow(0, insertCount)
      }

      val krefs       = kslice.columns.keys.toList.sorted
      val indexMapKey = IndexKey(indexNamePrefix, krefs, vrefs)

      // There are 3 cases:
      //  1) No entry in the indices: We sort the slice and add a SortedSlice entry.
      //  2) A SortedSlice entry in the index: We store it and the current slice in
      //     a JDBM Index and replace the entry with a SliceIndex.
      //  3) A SliceIndex entry in the index: We add the current slice in the JDBM
      //     index and update the SliceIndex entry.

      jdbmState.indices.get(indexMapKey) map {
        case sliceIndex: JDBM.SliceIndex =>
          (sliceIndex, jdbmState)

        case SortedSlice(indexName, kslice0, vslice0, vEncoder0, keyRefs, valRefs, count) =>
          val keyRowFormat                  = RowFormat.forSortingKey(krefs)
          val keyComparator                 = SortingKeyComparator(keyRowFormat, sortOrder.isAscending)
          val (dbFile, db, openedJdbmState) = jdbmState.opened()
          val storage                       = db.hashMap(indexName, keyComparator, ByteArraySerializer).create()
          val count                         = storeRows(kslice0, vslice0, keyRowFormat, vEncoder0, storage, 0)
          val sliceIndex                    = JDBM.SliceIndex(indexName, dbFile, storage, keyRowFormat, keyComparator, keyRefs, valRefs, count)

          (sliceIndex, openedJdbmState.copy(indices = openedJdbmState.indices + (indexMapKey -> sliceIndex), insertCount = count))

      } map {
        case (index, jdbmState) =>
          val newInsertCount = storeRows(kslice, vslice, index.keyRowFormat, vEncoder, index.storage, jdbmState.insertCount)

          // Although we have a global count of inserts, we also want to
          // specifically track counts on the index since some operations
          // may not use all indices (e.g. groupByN)
          val newIndex = index.copy(count = index.count + (newInsertCount - jdbmState.insertCount))

          jdbmState.copy(indices = jdbmState.indices + (indexMapKey -> newIndex), insertCount = newInsertCount)

      } getOrElse {
        // sort k/vslice and shove into SortedSlice.
        val indexName = indexMapKey.name
        val mvslice   = vslice.materialized
        val mkslice   = kslice.materialized

        // TODO Materializing after a sort may help w/ cache hits when traversing a column.
        val (vslice0, kslice0) = mvslice.sortWith(mkslice, sortOrder)
        val sortedSlice        = SortedSlice(indexName, kslice0, vslice0, vEncoder, krefs.toArray, vrefs.toArray, vslice0.size.toLong)

        jdbmState.copy(indices = jdbmState.indices + (indexMapKey -> sortedSlice), insertCount = 0)
      }
    }

    def loadTable(mergeEngine: MergeEngine[Bytes, SortBlockData], indices: IndexMap, sortOrder: DesiredSortOrder): Table = {
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

      Table(StreamT(Need(head)), ExactSize(totalCount)).transform(TransSpec1.DerefArray1)
    }

    override def join(left0: Table, right0: Table, orderHint: Option[JoinOrder])(leftKeySpec: TransSpec1,
                                                                                rightKeySpec: TransSpec1,
                                                                                joinSpec: TransSpec2): M[JoinOrder -> Table] = {

      def hashJoin(index: Slice, table: Table, flip: Boolean): NeedTable = {
        val (indexKeySpec, tableKeySpec) = if (flip) (rightKeySpec, leftKeySpec) else (leftKeySpec, rightKeySpec)

        val initKeyTrans  = composeSliceTransform(tableKeySpec)
        val initJoinTrans = composeSliceTransform2(joinSpec)

        def joinWithHash(stream: StreamT[M, Slice], keyTrans: SliceTransform1[_], joinTrans: SliceTransform2[_], hashed: HashedSlice): StreamT[M, Slice] = {

          StreamT(stream.uncons flatMap {
            case Some((head, tail)) =>
              keyTrans.advance(head) flatMap {
                case (keyTrans0, headKey) =>
                  val headBuf  = new ArrayIntList(head.size)
                  val indexBuf = new ArrayIntList(index.size)

                  val rowMap = hashed.mapRowsFrom(headKey)

                  @tailrec def loop(row: Int): Unit = if (row < head.size) {
                    rowMap(row) { indexRow =>
                      headBuf.add(row)
                      indexBuf.add(indexRow)
                    }
                    loop(row + 1)
                  }

                  loop(0)

                  val (index0, head0) = (index.remap(indexBuf), head.remap(headBuf))
                  val advancedM = if (flip) {
                    joinTrans.advance(head0, index0)
                  } else {
                    joinTrans.advance(index0, head0)
                  }
                  advancedM map {
                    case (joinTrans0, slice) =>
                      StreamT.Yield(slice, joinWithHash(tail, keyTrans0, joinTrans0, hashed))
                  }
              }

            case None =>
              Need(StreamT.Done)
          })
        }

        composeSliceTransform(indexKeySpec).advance(index) map {
          case (_, indexKey) =>
            val hashed = HashedSlice(indexKey)
            Table(joinWithHash(table.slices, initKeyTrans, initJoinTrans, hashed), UnknownSize)
        }
      }

      // TODO: Let ColumnarTableModule do this for super.join and have toInternalTable
      // take a transpec to compact by.

      val left1  = left0.compact(leftKeySpec)
      val right1 = right0.compact(rightKeySpec)

      if (yggConfig.hashJoins) {
        (left1.toInternalTable().toEither |@| right1.toInternalTable().toEither).tupled flatMap {
          case (Right(left), Right(right)) =>
            orderHint match {
              case Some(JoinOrder.LeftOrder) =>
                hashJoin(right.slice, left, flip = true) map (JoinOrder.LeftOrder -> _)
              case Some(JoinOrder.RightOrder) =>
                hashJoin(left.slice, right, flip = false) map (JoinOrder.RightOrder -> _)
              case _ =>
                hashJoin(right.slice, left, flip = true) map (JoinOrder.LeftOrder -> _)
            }

          case (Right(left), Left(right)) =>
            hashJoin(left.slice, right, flip = false) map (JoinOrder.RightOrder -> _)

          case (Left(left), Right(right)) =>
            hashJoin(right.slice, left, flip = true) map (JoinOrder.LeftOrder -> _)

          case (leftE, rightE) =>
            val idT           = Predef.identity[Table](_)
            val (left, right) = (leftE.fold(idT, idT), rightE.fold(idT, idT))
            super.join(left, right, orderHint)(leftKeySpec, rightKeySpec, joinSpec)
        }
      } else super.join(left1, right1, orderHint)(leftKeySpec, rightKeySpec, joinSpec)
    }

    def load(table: Table, apiKey: APIKey, tpe: JType): NeedTable
  }

  abstract class Table(slices: StreamT[M, Slice], size: TableSize) extends ColumnarTable(slices, size) {
    def companion = Table

    /**
      * Converts a table to an internal table, if possible. If the table is
      * already an `InternalTable` or a `SingletonTable`, then the conversion
      * will always succeed. If the table is an `ExternalTable`, then if it has
      * less than `limit` rows, it will be converted to an `InternalTable`,
      * otherwise it will stay an `ExternalTable`.
      */
    def toInternalTable(limit: Int = yggConfig.maxSliceSize): NeedEitherT[ExternalTable, InternalTable]

    /**
      * Forces a table to an external table, possibly de-optimizing it.
      */
    def toExternalTable: ExternalTable = new ExternalTable(slices, size)
  }

  class SingletonTable(slices0: StreamT[M, Slice]) extends Table(slices0, ExactSize(1)) with NoSortTable {
    // TODO assert that this table only has one row

    def toInternalTable(limit: Int): NeedEitherT[ExternalTable, InternalTable] =
      EitherT[Need, ExternalTable, InternalTable](slices.toStream map (slices1 => \/-(new InternalTable(Slice.concat(slices1.toList).takeRange(0, 1)))))

    def toRValue: M[RValue] = {
      def loop(stream: StreamT[M, Slice]): M[RValue] = stream.uncons flatMap {
        case Some((head, tail)) if head.size > 0 => Need(head.toRValue(0))
        case Some((_, tail))                     => loop(tail)
        case None                                => Need(CUndefined)
      }

      loop(slices)
    }

    def load(apiKey: APIKey, tpe: JType) = Table.load(this, apiKey, tpe)

    override def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): Need[Seq[Table]] = {
      val xform = transform(valueSpec)
      Need(List.fill(groupKeys.size)(xform))
    }

    override def takeRange(startIndex: Long, numberToTake: Long): Table =
      if (startIndex <= 0 && startIndex + numberToTake >= 1) this else Table.empty
  }

  /**
    * `InternalTable`s are tables that are *generally* small and fit in a single
    * slice and are completely in-memory. Because they fit in memory, we are
    * allowed more optimizations when doing things like joins.
    */
  class InternalTable(val slice: Slice) extends Table(singleStreamT(slice), ExactSize(slice.size)) {
    def toInternalTable(limit: Int): NeedEitherT[ExternalTable, InternalTable] =
      EitherT[Need, ExternalTable, InternalTable](Need(\/-(this)))

    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): M[Seq[Table]] =
      toExternalTable.groupByN(groupKeys, valueSpec, sortOrder, unique)

    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable       = toExternalTable.sort(sortKey, sortOrder)
    def sortUnique(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable = toExternalTable.sortUnique(sortKey, sortOrder)

    def load(apiKey: APIKey, tpe: JType): NeedTable = Table.load(this, apiKey, tpe)

    override def force: NeedTable         = Need(this)
    override def paged(limit: Int): Table = this

    override def takeRange(startIndex0: Long, numberToTake0: Long): Table = {
      if (startIndex0 > Int.MaxValue) {
        new InternalTable(Slice.empty)
      } else {
        val startIndex   = startIndex0.toInt
        val numberToTake = numberToTake0.toInt min Int.MaxValue
        new InternalTable(slice.takeRange(startIndex, numberToTake))
      }
    }
  }

  class ExternalTable(slices: StreamT[M, Slice], size: TableSize) extends Table(slices, size) {
    import Table.{ Table => _, _ }
    import SliceTransform.composeSliceTransform

    def load(apiKey: APIKey, tpe: JType) = Table.load(this, apiKey, tpe)

    def toInternalTable(limit0: Int): NeedEitherT[ExternalTable, InternalTable] = {
      val limit = limit0.toLong

      def acc(slices: StreamT[M, Slice], buffer: List[Slice], size: Long): M[ExternalTable \/ InternalTable] = {
        slices.uncons flatMap {
          case Some((head, tail)) =>
            val size0 = size + head.size
            if (size0 > limit) {
              val slices0 = Slice.concat((head :: buffer).reverse) :: tail
              val tableSize = this.size match {
                case EstimateSize(min, max) if min < size0 => EstimateSize(size0, max)
                case tableSize0                            => tableSize0
              }
              Need(-\/(new ExternalTable(slices0, tableSize)))
            } else {
              acc(tail, head :: buffer, head.size + size)
            }

          case None =>
            Need(\/-(new InternalTable(Slice.concat(buffer.reverse))))
        }
      }

      EitherT(acc(slices, Nil, 0L))
    }

    /**
      * Sorts the KV table by ascending or descending order of a transformation
      * applied to the rows.
      *
      * @see quasar.ygg.TableModule#sort(TransSpec1, DesiredSortOrder, Boolean)
      */
    def sort(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable =
      groupByN(Seq(sortKey), root, sortOrder, unique = false) map (_.headOption getOrElse Table.empty)

    def sortUnique(sortKey: TransSpec1, sortOrder: DesiredSortOrder): NeedTable =
      groupByN(Seq(sortKey), root, sortOrder, unique = true) map (_.headOption getOrElse Table.empty)

    /**
      * Sorts the KV table by ascending or descending order based on a seq of transformations
      * applied to the rows.
      *
      * @see quasar.ygg.TableModule#groupByN(TransSpec1, DesiredSortOrder, Boolean)
      */
    def groupByN(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, sortOrder: DesiredSortOrder, unique: Boolean): M[Seq[Table]] = {
      writeSorted(groupKeys, valueSpec, sortOrder, unique) map {
        case (streamIds, indices) =>
          val streams = indices.groupBy(_._1.streamId)
          streamIds.toStream map { streamId =>
            streams get streamId map (loadTable(sortMergeEngine, _, sortOrder)) getOrElse Table.empty
          }
      }
    }

    private def writeSorted(keys: Seq[TransSpec1], spec: TransSpec1, sort: DesiredSortOrder, uniq: Boolean): M[List[String] -> IndexMap] = uniq match {
      case false => writeSortedNonUnique(keys, spec, sort)
      case true  => writeSortedUnique(keys, spec, sort)
    }

    private def writeSortedUnique(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, order: DesiredSortOrder): Need[List[String] -> IndexMap] =
      writeTables(
        this transform root.spec slices,
        composeSliceTransform(valueSpec),
        groupKeys map composeSliceTransform,
        order
      )

    private def writeSortedNonUnique(groupKeys: Seq[TransSpec1], valueSpec: TransSpec1, order: DesiredSortOrder): Need[List[String] -> IndexMap] = {
      val keys1 = groupKeys map (kt => OuterObjectConcat(WrapObject(kt deepMap { case Leaf(_) => root(0) } spec, "0"), WrapObject(root(1), "1")))
      writeTables(
        this transform addGlobalId(root.spec) slices,
        composeSliceTransform(valueSpec deepMap { case Leaf(_) => TransSpec1.DerefArray0 } spec),
        keys1 map composeSliceTransform,
        order
      )
    }
  }
}
