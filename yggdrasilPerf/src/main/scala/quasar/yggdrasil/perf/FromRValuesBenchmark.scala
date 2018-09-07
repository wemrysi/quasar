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

import quasar.contrib.cats.effect.liftio._
import quasar.precog.common.{CValue, CLong, RArray, RObject, RValue}
import quasar.yggdrasil.table.TestColumnarTableModule

import cats.effect.IO

// Must not be in default package
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, State}
import org.openjdk.jmh.infra.Blackhole

import scalaz.WriterT
import scalaz.std.list._
import scalaz.syntax.applicative._
import shims._

/* Default settings for benchmarks in this class */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class FromRValuesBenchmark {

  val P = new TestColumnarTableModule {}

  @Param(value = Array("true", "false"))
  var streaming: Boolean = _

  @Param(value = Array("json", "materialize"))
  var consumption: String = _

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

  // Each chunk should look like:
  // { k1: v1, k2: v2, k3: v3 }, { k4: v1, k1: v2, k2: v3 }
  // don't set `keysPerObject` lower than `keys` or you will have duplicates.
  def scrollingFieldObjects(chunks: Int, chunkSize: Int, keys: Int, keysPerObject: Int, scalar: CValue): Stream[List[RValue]] =
    Stream.fill(chunks)(
      List.tabulate(chunkSize)(s =>
        RObject(List.tabulate(keysPerObject)(i =>
          ("k" + ((s * keysPerObject + i) % keys)) -> scalar
        ).toMap)))

  def createAndConsumeTable(data: Stream[List[RValue]], bh: Blackhole): IO[Unit] = {
    val table: WriterT[IO, List[IO[Unit]], P.Table] =
      if (streaming) {
        P.Table.fromQDataStream[WriterT[IO, List[IO[Unit]], ?], RValue](
          fs2.Stream.fromIterator[IO, fs2.Stream[IO, RValue]](
            data.iterator.map(fs2.Stream.emits(_).covary[IO])
          ).flatMap(x => x))
      } else {
        P.Table.fromRValues(data.flatten).pure[WriterT[IO, List[IO[Unit]], ?]]
      }
    table.run.flatMap { case (_, t) => SliceTools.consumeTable(P)(consumption, t, bh) }
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestLongs(bh: Blackhole): Unit = {
    createAndConsumeTable(scalars(10, 50000, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithFittingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(arrays(10, 500, 80, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithOverflowingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(arrays(10, 500, 200, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithFittingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(objects(10, 500, 80, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithOverflowingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(objects(10, 500, 200, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithDistinctColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(distinctFieldObjects(10, 500, 100, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithWideScrollingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(scrollingFieldObjects(10, 500, 200, 100, CLong(100)), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithNarrowScrollingColumns(bh: Blackhole): Unit = {
    createAndConsumeTable(scrollingFieldObjects(10, 500, 80, 50, CLong(100)), bh).unsafeRunSync
  }

}
