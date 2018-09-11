/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar
package yggdrasil
package perf

import quasar.contrib.fs2.convert
import quasar.yggdrasil.table._

import org.openjdk.jmh.infra.Blackhole

import cats.effect.IO
import scalaz.StreamT
import shims._

object SliceTools {

  def consumeSliceMaterialize(slice: Slice, bh: Blackhole): IO[Unit] =
    IO(bh.consume(slice.materialized))

  def consumeSlicesMaterialize(slices: StreamT[IO, Slice], bh: Blackhole): IO[Unit] =
    slices.foldLeftRec(IO.unit)((i, o) => i.flatMap(_ => consumeSliceMaterialize(o, bh))).flatMap(x => x)

  def consumeTableMaterialize(module: TestColumnarTableModule)(table: module.Table, bh: Blackhole): IO[Unit] =
    consumeSlicesMaterialize(table.slices, bh)

  def consumeTableJson(module: TestColumnarTableModule)(table: module.Table, bh: Blackhole): IO[Unit] =
    convert.fromStreamT(table.renderJson()).compile.drain

  def consumeTable(module: TestColumnarTableModule)(consumption: String, table: module.Table, bh: Blackhole): IO[Unit] =
    if (consumption == "json")
      consumeTableJson(module)(table, bh)
    else if (consumption == "materialize")
      consumeTableMaterialize(module)(table, bh)
    else
      sys.error(s"Invalid value for consumption: $consumption")

}
