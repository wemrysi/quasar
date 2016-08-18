package ygg.bench

import blueeyes._
import java.util.concurrent.TimeUnit
import org.openjdk.jmh.annotations._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 10, time = 200, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 200, timeUnit = TimeUnit.MILLISECONDS)
class ScalaVectorBenchmarker {
  def elems = 1 to 1000

  @Benchmark
  def scala_fold_sum(): Long = elems.foldLeft(0L)(_ + _)
}
