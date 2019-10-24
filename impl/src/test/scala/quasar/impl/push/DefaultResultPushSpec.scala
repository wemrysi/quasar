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

import cats.effect.IO

import eu.timepit.refined.auto._

import fs2.concurrent.SignallingRef
import fs2.job.JobManager
import fs2.{Stream, text}

import org.specs2.matcher.MatchResult

import quasar.api.QueryEvaluator
import quasar.api.destination.{Destination, DestinationType, ResultSink, ResultType}
import quasar.api.push.{PushMeta, RenderConfig, ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.{ConditionMatchers, EffectfulQSpec}

import scalaz.std.set._
import scalaz.std.string._
import scalaz.syntax.bind._
import scalaz.{Equal, NonEmptyList, \/-}

import shims.monadToScalaz

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  implicit val tmr = IO.timer(global)

  val TableId = 42
  val DestinationId = 42
  val RefDestinationType = DestinationType("ref", 1L)

  // convert a bytestream into stream of space separeted words
  def bytesToString(s: Stream[IO, Byte]): Stream[IO, String] =
    s.split(_ == 0x20).flatMap(Stream.chunk).through(text.utf8Decode)

  type Filesystem = Map[ResourcePath, String]

  final class RefDestination(ref: SignallingRef[IO, Filesystem]) extends Destination[IO] {
    def destinationType: DestinationType = RefDestinationType
    def sinks = NonEmptyList(csvSink)

    val csvSink: ResultSink[IO] = ResultSink.csv[IO](RenderConfig.Csv()) {
      case (dst, columns, bytes) =>
        bytesToString(bytes).evalMap(str =>
          ref.update(currentFs =>
            currentFs + (dst -> currentFs.get(dst).fold(str)(_ ++ str))))
    }
  }

  object RefDestination {
    def apply(): IO[(Destination[IO], SignallingRef[IO, Filesystem])] =
      for {
        fs <- SignallingRef[IO, Filesystem](Map.empty)
        destination = new RefDestination(fs)
      } yield (destination, fs)
  }

  final class MockResultRender extends ResultRender[IO, String] {
    def renderCsv(
        input: String,
        columns: List[TableColumn],
        config: RenderConfig.Csv,
        limit: Option[Long])
        : Stream[IO, Byte] =
      Stream(input).through(text.utf8Encode)

    def renderJson(
        input: String,
        prefix: String,
        delimiter: String,
        suffix: String)
        : Stream[IO, Byte] =
      Stream.empty
  }

  def mkEvaluator(fn: String => IO[Stream[IO, String]]): QueryEvaluator[IO, String, Stream[IO, String]] =
    new QueryEvaluator[IO, String, Stream[IO, String]] {
      def evaluate(query: String): IO[Stream[IO, String]] =
        fn(query)
    }

  def mkResultPush(
    tables: Map[Int, TableRef[String]],
    destinations: Map[Int, Destination[IO]],
    manager: JobManager[IO, Int, Nothing],
    evaluator: QueryEvaluator[IO, String, Stream[IO, String]])
      : IO[ResultPush[IO, Int, Int]] = {

    val lookupTable: Int => IO[Option[TableRef[String]]] =
      tableId => IO(tables.get(tableId))

    val lookupDestination: Int => IO[Option[Destination[IO]]] =
      destinationId => IO(destinations.get(destinationId))

    val render = new MockResultRender

    DefaultResultPush[IO, Int, Int, String, String](
      lookupTable,
      evaluator,
      lookupDestination,
      manager,
      render)
  }

  def mockEvaluate(q: String): String =
    s"evaluated($q)"

  val WorkTime = Duration(400, MILLISECONDS)
  val Timeout = 5 * WorkTime

  val await = IO.sleep(WorkTime * 2)
  val awaitS = Stream.sleep_(WorkTime)

  def latchGet(s: SignallingRef[IO, String], expected: String): IO[Unit] =
    s.discrete.filter(Equal[String].equal(_, expected)).take(1).compile.drain
      .timeoutTo(Timeout * 2, IO.raiseError(new Exception("latchGet timed out")))

  def waitForUpdate[K, V](s: SignallingRef[IO, Map[K, V]]): IO[Unit] =
    s.discrete.filter(_.nonEmpty).take(1).compile.drain
      .timeoutTo(Timeout, IO.raiseError(new Exception("waitForUpdate timed out")))

  /* Runs `io` with a timeout, producing a failing expectation if `io`
     completes before the timeout. errMsg is included with the failing expectation. */
  def verifyTimeout[A](io: IO[A], errMsg: String): IO[MatchResult[Any]] =
    (io >> IO(ko(errMsg))).timeoutTo(Timeout, IO(ok))

  "result push" >> {
    "push a table to a destination" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        (destination, filesystem) <- RefDestination()
        ref <- SignallingRef[IO, String]("Not started")
        evaluator = mkEvaluator(q => IO(Stream.emit(mockEvaluate(q)) ++ Stream.eval_(ref.set("Finished"))))

        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, evaluator)
        startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv, None)
        _ <- latchGet(ref, "Finished")
        filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
        _ <- cleanup
      } yield {
        filesystemAfterPush.keySet must equal(Set(pushPath))
        filesystemAfterPush(pushPath) must equal(mockEvaluate(query))
        startRes must beNormal
      }
    }

    "cancel a table push" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      def testStream(ref: SignallingRef[IO, String]): Stream[IO, String] =
        Stream("foo") ++
          Stream(" ") ++ // chunk delimiter
          Stream.eval_(ref.set("Working")) ++
          Stream.sleep_(WorkTime) ++
          Stream("bar") ++
          Stream.eval_(ref.set("Finished"))

      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        (destination, filesystem) <- RefDestination()
        ref <- SignallingRef[IO, String]("Not started")
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ => IO(testStream(ref))))
        startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv, None)
        _ <- latchGet(ref, "Working")
        cancelRes <- push.cancel(TableId)

        filesystemAfterPush <- waitForUpdate(filesystem) >> filesystem.get
        // fail the test if push evaluation was not cancelled
        evaluationFinished <- verifyTimeout(latchGet(ref, "Finished"), "Push not cancelled")
        _ <- cleanup
      } yield {
        filesystemAfterPush.keySet must equal(Set(pushPath))
        // check if a *partial* result was pushed
        filesystemAfterPush(pushPath) must equal("foo")
        startRes must beNormal
        cancelRes must beNormal
        evaluationFinished
      }
    }

    "retrieves the status of a running push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      for {
        (destination, filesystem) <- RefDestination()
        sync <- SignallingRef[IO, String]("Not started")
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ =>
          IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2"))))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- latchGet(sync, "Started")
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(PushMeta(DestinationId, _, ResultType.Csv, Status.Running(_), None))) => ok
        }
      }
    }

    "retrieves the status of a canceled push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      for {
        (destination, filesystem) <- RefDestination()
        sync <- SignallingRef[IO, String]("Not started")
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ =>
          IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2"))))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- latchGet(sync, "Started")
        _ <- push.cancel(TableId)
        _ <- await
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(PushMeta(DestinationId, _, ResultType.Csv, Status.Canceled(_, _), None))) => ok
        }
      }
    }

    "retrieves the status of a completed push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      for {
        (destination, filesystem) <- RefDestination()
        sync <- SignallingRef[IO, String]("Not started")
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ =>
          IO(Stream.eval_(sync.set("Started")) ++ awaitS ++ Stream.eval_(sync.set("Finished")))))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- latchGet(sync, "Finished")
        _ <- await // wait for concurrent update of push status
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(PushMeta(DestinationId, _, ResultType.Csv, Status.Finished(_, _), None))) => ok
        }
      }
    }

    "handles errors produced while computing the stream" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        (destination, filesystem) <- RefDestination()
        push <- mkResultPush(
          Map(TableId -> testTable),
          Map(DestinationId -> destination), jm, mkEvaluator(_ =>
            IO.raiseError[Stream[IO, String]](new Exception("boom"))))
        pushStatus <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- await // wait for error handling
        status <- push.status(TableId)
        _ <- cleanup
      } yield {
        status must beLike {
          case \/-(Some(PushMeta(DestinationId, _, ResultType.Csv, Status.Failed(ex, _, _), None))) =>
            ex.getMessage must equal("boom")
        }
      }
    }

    "retrieves the status of an errored push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())
      val ex = new Exception("boom")

      for {
        (destination, filesystem) <- RefDestination()
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        sync <- SignallingRef[IO, String]("Not started")
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ =>
          IO(Stream.eval_(sync.set("Started")) ++ Stream.raiseError[IO](ex))))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- latchGet(sync, "Started")
        _ <- await // wait for error handling
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(PushMeta(DestinationId, _, ResultType.Csv, Status.Failed(ex, _, _), None))) =>
            ex.getMessage must equal("boom")
        }
      }
    }

    "fails with ResultPushError.TableNotFound for unknown tables" >>* {
      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(), Map(), jm, mkEvaluator(_ => IO(Stream.empty)))
        pushStatus <- push.status(99)
        _ <- cleanup
      } yield {
        pushStatus must be_-\/(ResultPushError.TableNotFound(99))
      }
    }

    "returns None for a table that has not been pushed" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(), jm, mkEvaluator(_ => IO(Stream.empty)))
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must be_\/-(None)
      }
    }

    "rejects an already running push" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      for {
        (destination, filesystem) <- RefDestination()
        sync <- SignallingRef[IO, String]("Not started")
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(
          Map(TableId -> testTable),
          Map(DestinationId -> destination),
          jm,
          mkEvaluator(_ => IO(Stream.eval(sync.set("Started")).as("1") ++ awaitS.as("2") ++ Stream.eval(sync.set("Finished")).as("3"))))
        firstStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
        secondStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
        _ <- cleanup
      } yield {
        firstStartStatus must beNormal
        secondStartStatus must beAbnormal(ResultPushError.PushAlreadyRunning(TableId))
      }
    }

    "cancel all pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

      val path2 = ResourcePath.root() / ResourceName("bar")
      val testTable2 = TableRef(TableName("bar"), "queryBar", List())

      for {
        (destination, filesystem) <- RefDestination()
        refFoo <- SignallingRef[IO, String]("Not started")
        refBar <- SignallingRef[IO, String]("Not started")
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(1 -> testTable1, 2 -> testTable2), Map(DestinationId -> destination), jm, mkEvaluator {
          case "queryFoo" =>
            IO(Stream.eval_(refFoo.set("Started")) ++ awaitS ++ Stream.eval_(refFoo.set("Finished")))
          case "queryBar" =>
            IO(Stream.eval_(refBar.set("Started")) ++ awaitS ++ Stream.eval_(refBar.set("Finished")))
        })
        _ <- push.start(1, DestinationId, path1, ResultType.Csv, None)
        _ <- push.start(2, DestinationId, path2, ResultType.Csv, None)
        _ <- latchGet(refFoo, "Started") >> latchGet(refBar, "Started")
        _ <- push.cancelAll
        fooCanceled <- verifyTimeout(latchGet(refFoo, "Finished"), "queryFoo not cancelled")
        barCanceled <- verifyTimeout(latchGet(refBar, "Finished"), "queryBar not cancelled")
        _ <- cleanup
      } yield ok
    }

    "fails with ResultPush.DestinationNotFound with an unknown destination id" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())
      val UnknownDestinationId = 99

      for {
        (destination, filesystem) <- RefDestination()
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(), jm, mkEvaluator(_ => IO(Stream.empty)))
        pushRes <- push.start(TableId, UnknownDestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- cleanup
      } yield {
        pushRes must beAbnormal(ResultPushError.DestinationNotFound(UnknownDestinationId))
      }
    }

    "fails with ResultPush.TableNotFound with an unknown table id" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())
      val UnknownTableId = 99

      for {
        (destination, filesystem) <- RefDestination()
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(), Map(DestinationId -> destination), jm, mkEvaluator(_ => IO(Stream.empty)))
        pushRes <- push.start(UnknownTableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
        _ <- cleanup
      } yield {
        pushRes must beAbnormal(ResultPushError.TableNotFound(UnknownTableId))
      }
    }
  }
}
