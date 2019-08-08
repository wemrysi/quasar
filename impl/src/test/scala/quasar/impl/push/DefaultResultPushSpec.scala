/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.push

import slamdata.Predef._

import quasar.{ConditionMatchers, EffectfulQSpec}
import quasar.api.QueryEvaluator
import quasar.api.destination.DestinationType
import quasar.api.destination.ResultType
import quasar.api.push.ResultPush
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.connector.{Destination, ResultSink}
import quasar.api.resource.ResourcePath

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import cats.effect.IO
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.concurrent.SignallingRef
import fs2.job.JobManager
import fs2.{Stream, text}
import scalaz.{Equal, NonEmptyList}
import scalaz.std.set._
import scalaz.std.string._

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  implicit val tmr = IO.timer(global)

  val TableId = 42
  val DestinationId = 43
  val RefDestinationType = DestinationType("ref", 1L)

  def streamToString(s: Stream[IO, Byte]): IO[String] =
    s.through(text.utf8Decode).compile.toList.map(_.mkString)

  type Filesystem = Map[ResourcePath, String]

  val emptyFilesystem: Filesystem = Map.empty[ResourcePath, String]

  final class RefCsvSink(ref: Ref[IO, Filesystem]) extends ResultSink[IO] {
    type RT = ResultType.Csv[IO]

    val resultType = ResultType.Csv()

    def apply(dst: ResourcePath, result: (List[TableColumn], Stream[IO, Byte])): IO[Unit] = {
      val (columns, data) = result

      for {
        stringData <- streamToString(data)
        update <- ref.update(currentFs => (currentFs + (dst -> stringData)))
      } yield update
    }
  }

  final class RefDestination(ref: Ref[IO, Filesystem]) extends Destination[IO] {
    def destinationType: DestinationType = RefDestinationType
    def sinks = NonEmptyList(new RefCsvSink(ref))
  }

  def convert(st: Stream[IO, String]): Stream[IO, Byte] =
    st.through(text.utf8Encode)

  def mkEvaluator(fn: String => Stream[IO, String]): QueryEvaluator[IO, String, Stream[IO, String]] =
    new QueryEvaluator[IO, String, Stream[IO, String]] {
      def evaluate(query: String): IO[Stream[IO, String]] =
        IO(fn(query))
    }

  def mkResultPush(
    table: TableRef[String],
    destination: Destination[IO],
    manager: JobManager[IO, Int, Nothing],
    evaluator: QueryEvaluator[IO, String, Stream[IO, String]])
      : ResultPush[IO, Int, Int] = {
    val lookupTable: Int => IO[Option[TableRef[String]]] = {
      case 42 => IO(Some(table))
      case _ => IO(None)
    }

    val lookupDestination: Int => IO[Option[Destination[IO]]] = {
      case 42 => IO(Some(destination))
      case _ => IO(None)
    }

    DefaultResultPush[IO, Int, Int, String, Stream[IO, String]](
      lookupTable,
      evaluator,
      lookupDestination,
      manager,
      convert)
  }

  def mockEvaluate(q: String): String =
    s"evaluated($q)"

  def latchGet(s: SignallingRef[IO, String], expected: String): IO[Unit] =
    s.discrete.filter(Equal[String].equal(_, expected)).take(1).compile.drain

  "result push" >> {
    "push a table to a destination" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query something"
      val testTable = TableRef(TableName("foo"), query, List())

      for {
        filesystem <- Ref.of[IO, Filesystem](emptyFilesystem)
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        destination = new RefDestination(filesystem)
        ref <- SignallingRef[IO, String]("Not started")
        evaluator = mkEvaluator(q => Stream.emit(mockEvaluate(q)) ++ Stream.eval_(ref.set("Finished")))

        push = mkResultPush(testTable, destination, jm, evaluator)
        startRes <- push.start(42, 42, pushPath, ResultType.Csv[IO](), None)
        _ <- latchGet(ref, "Finished")
        filesystemAfterPush <- filesystem.get
        _ <- cleanup
      } yield {
        filesystemAfterPush.keySet must equal(Set(pushPath))
        filesystemAfterPush(pushPath) must equal(mockEvaluate(query))
        startRes must beNormal
      }
    }

    "cancel a table push" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query something"
      val testTable = TableRef(TableName("foo"), query, List())

      def testStream(ref: SignallingRef[IO, String]): Stream[IO, String] =
        Stream.eval(ref.set("Started")).as("foo") ++ Stream.sleep_(Duration(100, MILLISECONDS)) ++ Stream.eval(ref.set("Finished")).as("bar")

      for {
        filesystem <- Ref.of[IO, Filesystem](emptyFilesystem)
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        destination = new RefDestination(filesystem)
        ref <- SignallingRef[IO, String]("Not started")
        push = mkResultPush(testTable, destination, jm, mkEvaluator(_ => testStream(ref)))
        startRes <- push.start(42, 42, pushPath, ResultType.Csv[IO](), None)
        _ <- latchGet(ref, "Started")
        cancelRes <- push.cancel(42)
        filesystemAfterPush <- filesystem.get
        refAfter <- ref.get
        _ <- cleanup
      } yield {
        filesystemAfterPush.keySet must equal(Set.empty[ResourcePath])
        startRes must beNormal
        cancelRes must beNormal
        refAfter must equal("Started")
      }
    }
  }
}
