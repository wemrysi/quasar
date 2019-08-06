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

import quasar.Condition
import quasar.api.QueryEvaluator
import quasar.api.destination.DestinationType
import quasar.api.destination.ResultType
import quasar.api.push.ResultPush
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.connector.{Destination, ResultSink}
import quasar.api.resource.ResourcePath

import cats.effect.IO
import cats.effect.concurrent.Ref
import eu.timepit.refined.auto._
import fs2.{Stream, text}
import fs2.job.JobManager

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import scalaz.NonEmptyList

object DefaultResultPushSpec extends quasar.Qspec {
  implicit val cs = IO.contextShift(global)
  implicit val tmr = IO.timer(global)

  val TableId = 42
  val DestinationId = 43
  val RefDestinationType = DestinationType("ref", 1L)

  def streamToString(s: Stream[IO, Byte]): IO[String] =
    s.chunks.flatMap(Stream.chunk).through(text.utf8Decode).compile.toList.map(_.mkString)

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

  val evaluator = new QueryEvaluator[IO, String, String] {
    def evaluate(query: String): IO[String] =
      IO(query ++ " evaluated")
  }

  val convert: String => Stream[IO, Byte] =
    result => Stream.emit(result).covary[IO].through(text.utf8Encode)

  val jobManager: IO[JobManager[IO, Int, Nothing]] =
    JobManager[IO, Int, Nothing]().compile.lastOrError

  def mkResultPush(table: TableRef[String], destination: Destination[IO]): IO[ResultPush[IO, Int, Int]] = {
    val lookupTable: Int => IO[Option[TableRef[String]]] = {
      case 42 => IO(Some(table))
      case _ => IO(None)
    }

    val lookupDestination: Int => IO[Option[Destination[IO]]] = {
      case 42 => IO(Some(destination))
      case _ => IO(None)
    }

    jobManager.map(jm =>
      DefaultResultPush[IO, Int, Int, String, String](
        lookupTable,
        evaluator,
        lookupDestination,
        jm,
        convert))
  }

  "result push" >> {
    "push a table to a destination" >> {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")

      val testRun = for {
        filesystem <- Ref.of[IO, Filesystem](emptyFilesystem)
        table = TableRef(TableName("foo"), "query something", List())
        destination = new RefDestination(filesystem)
        push <- mkResultPush(table, destination)
        startRes <- push.start(42, 42, pushPath, ResultType.Csv[IO](), None)
        _ <- IO.sleep(Duration(1, SECONDS))
        filesystemAfterPush <- filesystem.get
      } yield (startRes, filesystemAfterPush)

      val (startRes, filesystemAfterPush) = testRun.unsafeRunSync

      filesystemAfterPush.keys mustEqual(Set(pushPath))
      filesystemAfterPush(pushPath) mustEqual("query something evaluated")
      startRes mustEqual Condition.normal()
    }
  }
}
