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

import quasar.yggdrasil.table._
import quasar.precog.common._

import cats.effect.IO

// Must not be in default package
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, State}
import org.openjdk.jmh.infra.Blackhole

/* Default settings for benchmarks in this class */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class FromRValuesBenchmark {

  @Param(value = Array("true", "false"))
  var streaming: Boolean = _

  def scalars(chunks: Int, chunkSize: Int, scalar: CValue): Stream[List[RValue]] =
    Stream.fill(chunks)(
      List.fill(chunkSize)(scalar))

  def arrays(chunks: Int, chunkSize: Int, arrSize: Int, scalar: CValue): Stream[List[RValue]] =
    Stream.fill(chunks)(
      List.fill(chunkSize)(
        RArray(List.tabulate(arrSize)(i => scalar))))

  // Each chunk should look like:
  // { k1: v1, k2: v2, k3: v3 }, { k1: v1, k2: v2, k3: v3 }
  def objects(chunks: Int, chunkSize: Int, keys: Int, scalar: CValue): Stream[List[RValue]] =
    Stream.fill(chunks)(
      List.fill(chunkSize)(
        RObject(List.tabulate(keys)(i => ("k" + i) -> scalar).toMap)))

  // Each chunk should look like:
  // { k11: v1, k12: v2, k13: v3 }, { k21: v1, k22: v2, k23: v3 }
  def distinctFieldObjects(chunks: Int, chunkSize: Int, keys: Int, scalar: CValue): Stream[List[RValue]] =
    Stream.tabulate(chunks)(c =>
      List.tabulate(chunkSize)(s =>
        RObject(List.tabulate(keys)(i => ("k" + c + s + i) -> scalar).toMap)))

  def createAndConsumeSlices(data: Stream[List[RValue]], bh: Blackhole): IO[Unit] = {
    val slices: fs2.Stream[IO, Slice] =
      if (streaming) {
        Slice.allFromRValues(fs2.Stream.fromIterator[IO, fs2.Stream[IO, RValue]](data.iterator.map(fs2.Stream.emits(_).covary)).flatMap(x => x))
      } else {
        fs2.Stream.fromIterator[IO, Slice](data.iterator.map(l => Slice.fromRValues(l.toStream)))
      }
    SliceTools.consumeSlices(slices, bh)
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestLongs(bh: Blackhole): Unit = {
    createAndConsumeSlices(scalars(20, 50000, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithFittingColumns(bh: Blackhole): Unit = {
    createAndConsumeSlices(arrays(20, 200, 80, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithOverflowingColumns(bh: Blackhole): Unit = {
    createAndConsumeSlices(arrays(20, 200, 200, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithOverflowingColumns(bh: Blackhole): Unit = {
    createAndConsumeSlices(objects(20, 200, 200, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithFittingColumns(bh: Blackhole): Unit = {
    createAndConsumeSlices(objects(20, 200, 80, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithDistinctColumns(bh: Blackhole): Unit = {
    createAndConsumeSlices(distinctFieldObjects(20, 200, 100, CLong(100)), bh).unsafeRunSync
  }

}
