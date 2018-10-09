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
import scalaz.syntax.monad._
import shims._

import java.nio.ByteBuffer

/* Default settings for benchmarks in this class */
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class TableConstructionBenchmark {

  val P = new TestColumnarTableModule {}

  @Param(value = Array("old", "streaming", "tectonic"))
  var mode: String = _

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

  def createAndConsumeTable(data: fs2.Stream[IO, Byte], bh: Blackhole): IO[Unit] = {
    def parseWithJawn: fs2.Stream[IO, RValue] = {
      implicit val facade = qdata.json.QDataFacade.qdata[RValue]
      val parser = jawn.AsyncParser[RValue](jawn.AsyncParser.ValueStream)

      val absorbtion =
        data.chunks.scan(fs2.Chunk.empty[RValue]) { (_, chunk) =>
          fs2.Chunk.seq(parser.absorb(chunk.toByteBuffer).right.get)
        }

      absorbtion.flatMap(fs2.Stream.chunk(_)) ++
        fs2.Stream.chunk(fs2.Chunk.seq(parser.finish().right.get))
    }

    val table: WriterT[IO, List[IO[Unit]], P.Table] = if (mode == "streaming") {
      P.Table.fromQDataStream[WriterT[IO, List[IO[Unit]], ?], RValue](
        parseWithJawn)
    } else if (mode == "old") {
      parseWithJawn.compile.to[Stream].map(P.Table.fromRValues(_, None)).liftM[WriterT[?[_], List[IO[Unit]], ?]]
    } else if (mode == "tectonic") {
      P.Table.parseJson[WriterT[IO, List[IO[Unit]], ?]](data)
    } else {
      sys.error("invalid mode")
    }

    table.run flatMap { case (_, t) => SliceTools.consumeTableJson(P)(t, bh) }
  }

  def dataToGiantStrings(data: Stream[List[RValue]]): fs2.Stream[IO, Byte] = {
    fs2.Stream suspend {
      val bytes = data.map(_.map(_.toJValue.renderCompact).mkString("\n").getBytes).toList

      fs2.Stream
        .emits(bytes.map(ByteBuffer.wrap).map(fs2.Chunk.byteBuffer))
        .flatMap(fs2.Stream.chunk(_))
        .covary[IO]
    }
  }

  val longsData = dataToGiantStrings(scalars(10, 50000, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestLongs(bh: Blackhole): Unit = createAndConsumeTable(longsData, bh).unsafeRunSync

  val arrsWithFittingColumnsData = dataToGiantStrings(arrays(10, 500, 80, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithFittingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(arrsWithFittingColumnsData, bh).unsafeRunSync

  val arrsWithOverflowingColumnsData = dataToGiantStrings(arrays(10, 500, 200, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestArrsWithOverflowingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(arrsWithOverflowingColumnsData, bh).unsafeRunSync

  val objectsWithFittingColumnsData = dataToGiantStrings(objects(10, 500, 80, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithFittingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(objectsWithFittingColumnsData, bh).unsafeRunSync

  val objectsWithOverflowingColumnsData = dataToGiantStrings(objects(10, 500, 200, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithOverflowingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(objectsWithOverflowingColumnsData, bh).unsafeRunSync

  val objectsWithDistinctColumnsData = dataToGiantStrings(distinctFieldObjects(10, 500, 100, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithDistinctColumns(bh: Blackhole): Unit =
    createAndConsumeTable(objectsWithDistinctColumnsData, bh).unsafeRunSync

  val objectsWithWideScrollingColumnsData = dataToGiantStrings(scrollingFieldObjects(10, 500, 200, 100, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithWideScrollingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(objectsWithWideScrollingColumnsData, bh).unsafeRunSync

  val objectsWithNarrowScrollingColumnsData = dataToGiantStrings(scrollingFieldObjects(10, 500, 80, 50, CLong(100)))

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def ingestObjectsWithNarrowScrollingColumns(bh: Blackhole): Unit =
    createAndConsumeTable(objectsWithNarrowScrollingColumnsData, bh).unsafeRunSync
}
