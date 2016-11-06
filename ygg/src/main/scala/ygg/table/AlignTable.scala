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
import ygg._, common._, data._

object AlignTable {
  sealed trait AlignState                                                                 extends Product with Serializable
  final case class RunLeft(rightRow: Int, rightKey: Slice, rightAuthority: Option[Slice]) extends AlignState
  final case class RunRight(leftRow: Int, leftKey: Slice, rightAuthority: Option[Slice])  extends AlignState
  final case class FindEqualAdvancingRight(leftRow: Int, leftKey: Slice)                  extends AlignState
  final case class FindEqualAdvancingLeft(rightRow: Int, rightKey: Slice)                 extends AlignState

  sealed trait Span           extends Product with Serializable
  final case object LeftSpan  extends Span
  final case object RightSpan extends Span
  final case object NoSpan    extends Span

  sealed trait NextStep                                                       extends Product with Serializable
  final case class MoreLeft(span: Span, leq: BitSet, ridx: Int, req: BitSet)  extends NextStep
  final case class MoreRight(span: Span, lidx: Int, leq: BitSet, req: BitSet) extends NextStep

  def apply[T](rep: TableRep[T], alignL: TransSpec1, sourceR: T, alignR: TransSpec1): PairOf[T] = {
    val sourceL = rep.table
    import rep.companion._

    // we need a custom row comparator that ignores the global ID introduced to prevent elimination of
    // duplicate rows in the write to JDBM
    def buildRowComparator(lkey: Slice, rkey: Slice, rauth: Slice): RowComparator = new RowComparator {
      private def make(rhs: Slice) = Slice.rowComparatorFor(lkey deref 0, rhs deref 0)(_.columns.keys map (_.selector))
      private val mainComparator   = make(rkey)
      private val auxComparator    = if (rauth == null) null else make(rauth)

      def compare(i1: Int, i2: Int) =
        if (i2 < 0 && rauth != null) auxComparator.compare(i1, rauth.size + i2) else mainComparator.compare(i1, i2)
    }

    // this method exists only to skolemize A and B
    def writeStreams[A, B](left: NeedSlices,
                           leftKeyTrans: SliceTransform1[A],
                           right: NeedSlices,
                           rightKeyTrans: SliceTransform1[B],
                           leftWriteState: JDBMState,
                           rightWriteState: JDBMState): LazyPairOf[T] = {

      // We will *always* have a lhead and rhead, because if at any point we
      // run out of data, we'll still be hanging on to the last slice on the
      // other side to use as the authority for equality comparisons
      def step(
          state: AlignState,
          lhead: Slice,
          ltail: NeedSlices,
          stepleq: BitSet,
          rhead: Slice,
          rtail: NeedSlices,
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
                } getOrElse rbs.point[Need]) map { (lbs, _) }
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
                    } getOrElse lbs.point[Need]) map { (_, rbs) }

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
              (empty, empty).point[Need]
          }

        case None =>
          //println("uncons left returned none")
          (empty, empty).point[Need]
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
      initState).value
  }
}
