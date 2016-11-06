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

import ygg._, common._

sealed abstract class BaseTable(val slices: NeedSlices, val size: TableSize) extends ygg.table.Table {
  self =>

  override def toString = s"Table(_, $size)"

  def sample(size: Int, specs: Seq[TransSpec1]): M[Seq[Table]] = Sampling.sample(self, size, specs)
  def projections: Map[Path, Projection]                       = Map()
  def withProjections(ps: Map[Path, Projection]): BaseTable    = new ProjectionsTable(this, projections ++ ps)
}

/**
  * `InternalTable`s are tables that are *generally* small and fit in a single
  * slice and are completely in-memory. Because they fit in memory, we are
  * allowed more optimizations when doing things like joins.
  */

final class InternalTable(val slice: Slice)                                                          extends BaseTable(singleStreamT(slice), ExactSize(slice.size))
final class ProjectionsTable(val underlying: Table, override val projections: Map[Path, Projection]) extends BaseTable(underlying.slices, underlying.size)
final class ExternalTable(slices: NeedSlices, size: TableSize)                                       extends BaseTable(slices, size)
