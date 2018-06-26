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
import fs2.{Chunk, Stream}

// Must not be in default package
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, Setup, State}
import org.openjdk.jmh.infra.Blackhole
import org.openjdk.jmh.runner.{Runner, RunnerException}
import org.openjdk.jmh.runner.options.{Options, OptionsBuilder}

/* Default settings for benchmarks in this class */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class FromRValuesBenchmark {

  def scalars(numChunks: Int, chunkSize: Int, scalar: CValue): Stream[IO, RValue] =
    List.fill(numChunks)(
      Stream.emits(Array.fill(chunkSize)(scalar)))
    .foldLeft(Stream.empty.covaryAll[IO, RValue])(_ ++ _)

  def arrays(chunks: Int, chunkSize: Int, arrSize: Int, scalar: CValue): Stream[IO, RValue] =
    List.fill(chunks)(
      Stream.emits(List.fill(chunkSize)(
        RArray(List.tabulate(arrSize)(i => scalar)))))
    .foldLeft(Stream.empty.covaryAll[IO, RValue])(_ ++ _)

  // Each chunk should look like:
  // { k1: v1, k2: v2, k3: v3 }, { k1: v1, k2: v2, k3: v3 }
  def objects(chunks: Int, chunkSize: Int, keys: Int, scalar: CValue): Stream[IO, RValue] = {
    List.fill(chunks)(
      Stream.emits(List.fill(chunkSize)(
        RObject(List.tabulate(keys)(i => ("k" + i) -> scalar).toMap))))
    .foldLeft(Stream.empty.covaryAll[IO, RValue])(_ ++ _)
  }

  // Each chunk should look like:
  // { k11: v1, k12: v2, k13: v3 }, { k21: v1, k22: v2, k23: v3 }
  def distinctFieldObjects(chunks: Int, chunkSize: Int, keys: Int, scalar: CValue): Stream[IO, RValue] =
    List.tabulate(chunks)(c =>
      Stream.emits(List.tabulate(chunkSize)(s =>
        RObject(List.tabulate(keys)(i => ("k" + c + s + i) -> scalar).toMap)))
    ).foldLeft(Stream.empty.covaryAll[IO, RValue])(_ ++ _)

  def consumeSlice(slice: Slice, bh: Blackhole): IO[Unit] = {
    IO(bh.consume(slice.materialized))
  }

  def consumeSlices(slices: Stream[IO, Slice], bh: Blackhole): IO[Unit] = {
    slices.compile.fold(())((_, o) => consumeSlice(o, bh))
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestLongs(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(scalars(20, 50000, CLong(100))), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithFittingColumns(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(arrays(20, 20, 80, CLong(100))), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithOverflowingColumns(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(arrays(20, 20, 200, CLong(100))), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithOverflowingColumns(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(objects(20, 20, 200, CLong(100))), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithFittingColumns(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(objects(20, 20, 80, CLong(100))), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithDistinctColumns(bh: Blackhole): Unit = {
    consumeSlices(Slice.allFromRValues(distinctFieldObjects(20, 20, 100, CLong(100))), bh).unsafeRunSync
  }

}
