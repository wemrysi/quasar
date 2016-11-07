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

import ygg._, common._, data._, trans._
import scalaz.{ Source => _, _ }, Scalaz._, Ordering._

object CogroupTable {
  final case class SliceId(id: Int) {
    def +(n: Int): SliceId = SliceId(id + n)
  }

  def apply[T: TableRep](self: T, leftKey: TransSpec1, rightKey: TransSpec1, that: T)(leftResultTrans: TransSpec1, rightResultTrans: TransSpec1, bothResultTrans: TransSpec2): T = {
    val companion = companionOf[T]

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
                                bothTransform: SliceTransform2[BR]): Need[(Slice, LR, RR, BR)] = {

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
    def cogroup0[LK, RK, LR, RR, BR](self: T,
                                     stlk: SliceTransform1[LK],
                                     strk: SliceTransform1[RK],
                                     stlr: SliceTransform1[LR],
                                     strr: SliceTransform1[RR],
                                     stbr: SliceTransform2[BR]): T = {
      val stateAdt = new CogroupStateAdt[LK, RK, LR, RR, BR]
      import stateAdt._

      // step is the continuation function fed to uncons. It is called once for each emitted slice
      def step(state: CogroupState): Need[Option[Slice -> CogroupState]] = {
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

          def continue(nextStep: NextStep[LK, RK]): Need[Option[Slice -> CogroupState]] = nextStep match {
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
        leftUnconsed  <- self.compact(leftKey, AnyDefined).slices.uncons
        rightUnconsed <- that.compact(rightKey, AnyDefined).slices.uncons

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

      companion(
        StreamT.wrapEffect(initialState map (state => unfoldStream(state getOrElse CogroupDone)(step))),
        UnknownSize
      )
    }

    cogroup0(
      self,
      composeSliceTransform(leftKey),
      composeSliceTransform(rightKey),
      composeSliceTransform(leftResultTrans),
      composeSliceTransform(rightResultTrans),
      composeSliceTransform2(bothResultTrans))
  }

  final case class SlicePosition[K](
    sliceId: SliceId,
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
    tail: NeedSlices
  )

  sealed trait NextStep[A, B] extends Product with Serializable

  final case class SplitLeft[A, B](lpos: Int)  extends NextStep[A, B]
  final case class SplitRight[A, B](rpos: Int) extends NextStep[A, B]

  final case class NextCartesianLeft[A, B](
    left: SlicePosition[A],
    right: SlicePosition[B],
    rightStart: Option[SlicePosition[B]],
    rightEnd: Option[SlicePosition[B]]
  ) extends NextStep[A, B]

  final case class NextCartesianRight[A, B](
    left: SlicePosition[A],
    right: SlicePosition[B],
    rightStart: Option[SlicePosition[B]],
    rightEnd: Option[SlicePosition[B]]
  ) extends NextStep[A, B]

  final case class SkipRight[A, B](left: SlicePosition[A], rightEnd: SlicePosition[B])                                  extends NextStep[A, B]
  final case class RestartRight[A, B](left: SlicePosition[A], rightStart: SlicePosition[B], rightEnd: SlicePosition[B]) extends NextStep[A, B]

  final class CogroupStateAdt[LK, RK, LR, RR, BR] {
    sealed trait CogroupState extends Product with Serializable
    final case class EndLeft(lr: LR, lhead: Slice, ltail: NeedSlices)  extends CogroupState
    final case class EndRight(rr: RR, rhead: Slice, rtail: NeedSlices) extends CogroupState
    final case object CogroupDone                                      extends CogroupState
    final case class Cogroup(
      lr: LR,
      rr: RR,
      br: BR,
      left: SlicePosition[LK],
      right: SlicePosition[RK],
      rightStart: Option[SlicePosition[RK]],
      rightEnd: Option[SlicePosition[RK]]
    ) extends CogroupState
  }
}
