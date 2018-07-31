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
import quasar.fp.ski.κ
import quasar.precog.common.{CLong, CPath, RArray, RObject, RValue}
import quasar.yggdrasil.table.TestColumnarTableModule

import cats.effect.IO

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, Setup, State}
import org.openjdk.jmh.infra.Blackhole

import scalaz._, Scalaz._
import shims._

object LeftShiftBenchmark {
  val P = new TestColumnarTableModule {}

  def scrolling(nrKeysPerObject: Int, totalNrKeys: Int): Int => Option[RValue] = { r =>
    assert(totalNrKeys >= nrKeysPerObject)
    RObject(List.tabulate(nrKeysPerObject)(i => ("k" + ((r + i) % totalNrKeys)) -> CLong(i + r + 42)).toMap).some
  }

  val scalarUndefined: Int => Option[RValue] = { i =>
    (i % 2) match {
      case 0 => none
      case _ => CLong(i + 42).some
    }
  }

  def leftShiftRow(key: Int, value: Option[RValue]): RValue =
    value match {
      case None => RArray(CLong(key))
      case Some(v) => RArray(CLong(key), v)
    }

  def leftShiftTestData(chunks: Int, chunkSize: Int, value: Int => Option[RValue]): Stream[List[RValue]] =
    Stream.tabulate(chunks)(c =>
      List.tabulate(chunkSize){ s =>
        val i = (c * chunkSize) + s
        leftShiftRow(i, value(i))
      })

  def mkTable(module: TestColumnarTableModule)(data: Stream[List[RValue]]): IO[module.Table] =
    module.Table.fromRValueStream[WriterT[IO, List[IO[Unit]], ?]](
      fs2.Stream.fromIterator[IO, fs2.Stream[IO, RValue]](
        data.iterator.map(fs2.Stream.emits(_).covary[IO])
      ).join).run.map(_._2)

  abstract class BenchmarkState(v: Int => Option[RValue]) {
    @Param(Array("5"))
    var chunks: Int = _

    @Param(Array("10", "10000", "1000000"))
    var chunkSize: Int = _

    @Param(Array("true", "false"))
    var emitOnUndef: Boolean = _

    var table: IO[P.Table] = _

    @Setup
    def setup(): Unit = {
      table = mkTable(P)(leftShiftTestData(chunks, chunkSize, v)).flatMap(_.force)
    }
  }

  @State(Scope.Benchmark)
  class BenchmarkState_scrolling extends BenchmarkState(scrolling(nrKeysPerObject = 10, totalNrKeys = 14))

  @State(Scope.Benchmark)
  class BenchmarkState_scalarUndefined extends BenchmarkState(scalarUndefined)

}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
class LeftShiftBenchmark {
  import LeftShiftBenchmark._

  @Param(Array("5"))
  var chunks: Int = _

  @Param(Array("10", "10000", "1000000"))
  var chunkSize: Int = _

  @Param(Array("10"))
  var arraySize: Int = _

  @Param(Array("10"))
  var nrKeysPerObject: Int = _

  // make sure that: totalNrKeys >= nrKeysPerObject
  @Param(Array("14"))
  var totalNrKeys: Int = _

  def leftShift(table: P.Table, emitOnUndef: Boolean): P.Table =
    table.leftShift(CPath.Identity \ 1, emitOnUndef)

  def doTest(v: Int => Option[RValue], bh: Blackhole): Unit = {
    val table: IO[P.Table] = mkTable(P)(leftShiftTestData(chunks, chunkSize, v))
    val tableAfter: IO[P.Table] = table.map(t => leftShift(t, true))
    tableAfter.map(t => SliceTools.consumeTable(P)(t, bh)).unsafeRunSync
  }

  def doTestForce(state: BenchmarkState, bh: Blackhole): Unit = {
    val table: IO[P.Table] = state.table
    val tableAfter: IO[P.Table] = table.map(t => leftShift(t, state.emitOnUndef))
    tableAfter.map(t => SliceTools.consumeTable(P)(t, bh)).unsafeRunSync
  }

  @Benchmark
  def arrays(bh: Blackhole): Unit = {
    val v: Int => Option[RValue] =
      κ(RArray(List.tabulate(arraySize)(i => CLong(i + 42))).some)
    doTest(v, bh)
  }

  @Benchmark
  def objects(bh: Blackhole): Unit = {
    val v: Int => Option[RValue] =
      κ(RObject(List.tabulate(nrKeysPerObject)(i => ("k" + i) -> CLong(i + 42)).toMap).some)
    doTest(v, bh)
  }

  @Benchmark
  def distinctFieldsObjects(bh: Blackhole): Unit = {
    val v: Int => Option[RValue] = { r =>
      RObject(List.tabulate(nrKeysPerObject)(i => ("k" + r + i) -> CLong(i + r + 42)).toMap).some
    }
    doTest(v, bh)
  }

  @Benchmark
  def scrollingFieldsObjects(bh: Blackhole): Unit = {
    doTest(scrolling(nrKeysPerObject, totalNrKeys), bh)
  }

  @Benchmark
  def scrollingFieldsObjectsForced(state: BenchmarkState_scrolling, bh: Blackhole): Unit = {
    doTestForce(state, bh)
  }

  @Benchmark
  def heterogeneous(bh: Blackhole): Unit = {
    val v: Int => Option[RValue] = { i =>
      (i % 4) match {
        case 0 => none
        case 1 => CLong(i + 42).some
        case 2 => RArray(List.tabulate(arraySize)(i => CLong(i + 42))).some
        case _ => RObject(List.tabulate(nrKeysPerObject)(i => ("k" + i) -> CLong(i + 42)).toMap).some
      }
    }
    doTest(v, bh)
  }

  @Benchmark
  def scalarOrUndefined(bh: Blackhole): Unit = {
    doTest(scalarUndefined, bh)
  }

  @Benchmark
  def scalarOrUndefinedForced(state: BenchmarkState_scalarUndefined, bh: Blackhole): Unit = {
    doTestForce(state, bh)
  }
}
