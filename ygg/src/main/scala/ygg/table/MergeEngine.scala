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
import ygg._, common._
import JDBM._

class MergeEngine {
  type BlockData = BlockProjectionData[Bytes]
  type SuccF     = Bytes => Need[Option[BlockData]]

  case class CellState(index: Int, maxKey: Bytes, slice0: Slice, succf: SuccF, remap: Array[Int], position: Int) {
    def toCell = new Cell(index, maxKey, slice0)(succf, remap.clone, position)
  }
  object CellState {
    def apply(index: Int, maxKey: Bytes, slice0: Slice, succf: SuccF) =
      new CellState(index, maxKey, slice0, succf, new Array[Int](slice0.size), 0)
  }

  /**
    * A wrapper for a slice, and the function required to get the subsequent
    * block of data.
    */
  case class Cell private[MergeEngine] (index: Int, maxKey: Bytes, slice0: Slice)(succf: SuccF,
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

    def succ: Need[Option[CellState]] = {
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
      systemArraycopy(remap, 0, remap0, 0, slice0.size)
      new CellState(index, maxKey, slice0, succf, remap0, position)
    }
  }

  sealed trait CellMatrix { self =>
    def cells: Iterable[Cell]
    def compare(cl: Cell, cr: Cell): Ordering

    lazy val ordering: Ord[Cell] = Ord.order[Cell](compare(_, _))
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
        private[this] val allCells: scmMap[Int, Cell] = initialCells.map(c => (c.index, c))(breakOut)
        private[this] val comparatorMatrix            = fillMatrix(initialCells)

        def cells = allCells.values

        def compare(cl: Cell, cr: Cell): Ordering = {
          comparatorMatrix(cl.index)(cr.index).compare(cl.position, cr.position)
        }
      }
    }
  }

  def mergeProjections(inputSortOrder: DesiredSortOrder, cellStates: Stream[CellState])(keyf: Slice => Iterable[CPath]): NeedSlices = {
    // dequeues all equal elements from the head of the queue
    @inline
    @tailrec
    def dequeueEqual(
        queue: scmPriorityQueue[Cell],
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
    def consumeToBoundary(queue: scmPriorityQueue[Cell], cellMatrix: CellMatrix, idx: Int): Int -> List[Cell] = {
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

    StreamT.unfoldM[Need, Slice, Stream[CellState]](cellStates) { cellStates =>
      val cells: Vec[Cell] = cellStates.map(_.toCell).toVector //(breakOut)

      // TODO: We should not recompute all of the row comparators every time,
      // since all but one will still be valid and usable. However, getting
      // to this requires more significant rework than can be undertaken
      // right now.
      val cellMatrix = CellMatrix(cells)(keyf)

      val ord: Order[Cell] = (
        if (inputSortOrder.isAscending)
          cellMatrix.ordering.reverseOrder
        else
          cellMatrix.ordering
      )
      val queue = scmPriorityQueue(cells: _*)(ord.toScalaOrdering)

      val (finishedSize, expired) = consumeToBoundary(queue, cellMatrix, 0)

      if (expired.isEmpty) {
        Need(None)
      }
      else {
        val completeSlices    = expired.map(_.slice)
        val pairs0: Vec[Cell] = queue.dequeueAll
        val pairs             = pairs0.map(_.split)
        val prefixes          = pairs map (_._1)
        val suffixes          = pairs map (_._2)

        val emission = Slice(
          finishedSize,
          (completeSlices.flatMap(_.columns) ++ prefixes.flatMap(_.columns)).groupBy(_._1) map {
            case (ref, columns) =>
              (columns.size match {
                case 1 => columns.head
                case _ => ref -> ArraySetColumn(ref.ctype, columns.map(_._2).toArray)
              }): ColumnRef -> Column
          }
        )

        val successorStatesM = expired.map(_.succ).sequence.map(_.toStream.flatten)

        successorStatesM map (ss => Some((emission, ss ++ suffixes)))
      }
    }
  }
}
