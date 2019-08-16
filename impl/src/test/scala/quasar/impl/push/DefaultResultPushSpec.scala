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

import quasar.api.QueryEvaluator
import quasar.api.destination.DestinationType
import quasar.api.destination.ResultType
import quasar.api.push.{ResultPush, ResultPushError, Status}
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.connector.{Destination, ResultSink}
import quasar.{ConditionMatchers, EffectfulQSpec}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import cats.effect.IO
import eu.timepit.refined.auto._
import fs2.concurrent.SignallingRef
import fs2.job.JobManager
import fs2.{Stream, text}
import org.specs2.matcher.MatchResult
import scalaz.std.set._
import scalaz.std.string._
import scalaz.syntax.bind._
import scalaz.{Equal, NonEmptyList, \/-}
import shims._

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  implicit val tmr = IO.timer(global)

  val TableId = 42
  val DestinationId = 42
  val RefDestinationType = DestinationType("ref", 1L)

  // convert a bytestream into stream of space separeted words
  def bytesToString(s: Stream[IO, Byte]): Stream[IO, String] =
    s.split(_ == 0x20).flatMap(Stream.chunk).through(text.utf8Decode)

  type Filesystem = Map[ResourcePath, String]

  // a sink that writes to a Ref
  final class RefCsvSink(ref: SignallingRef[IO, Filesystem]) extends ResultSink[IO] {
    type RT = ResultType.Csv[IO]

    val resultType = ResultType.Csv()

    def apply(dst: ResourcePath, result: (List[TableColumn], Stream[IO, Byte])): IO[Unit] = {
      val (columns, data) = result

      // write stream one word at a time to ref
      bytesToString(data).evalMap(str =>
        ref.update(currentFs =>
          currentFs + (dst -> currentFs.get(dst).fold(str)(_ ++ str)))).compile.drain
    }
  }

  final class RefDestination(ref: SignallingRef[IO, Filesystem]) extends Destination[IO] {
    def destinationType: DestinationType = RefDestinationType
    def sinks = NonEmptyList(new RefCsvSink(ref))
  }

  object RefDestination {
    def apply(): IO[(Destination[IO], SignallingRef[IO, Filesystem])] =
      for {
        fs <- SignallingRef[IO, Filesystem](Map.empty)
        destination = new RefDestination(fs)
      } yield (destination, fs)
  }

  def convert(st: Stream[IO, String]): Stream[IO, Byte] =
    st.through(text.utf8Encode)

  def mkEvaluator(fn: String => Stream[IO, String]): QueryEvaluator[IO, String, Stream[IO, String]] =
    new QueryEvaluator[IO, String, Stream[IO, String]] {
      def evaluate(query: String): IO[Stream[IO, String]] =
        IO(fn(query))
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

    DefaultResultPush[IO, Int, Int, String, Stream[IO, String]](
      lookupTable,
      evaluator,
      lookupDestination,
      manager,
      convert)
  }

  def mockEvaluate(q: String): String =
    s"evaluated($q)"

  val WorkTime = Duration(200, MILLISECONDS)
  val Timeout = 5 * WorkTime

  val await = IO.sleep(WorkTime)
  val awaitS = Stream.sleep_(WorkTime)

  def latchGet(s: SignallingRef[IO, String], expected: String): IO[Unit] =
    s.discrete.filter(Equal[String].equal(_, expected)).take(1).compile.drain.timeout(Timeout)

  def waitForUpdate[K, V](s: SignallingRef[IO, Map[K, V]]): IO[Unit] =
    s.discrete.filter(_.nonEmpty).take(1).compile.drain.timeout(Timeout)

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
        evaluator = mkEvaluator(q => Stream.emit(mockEvaluate(q)) ++ Stream.eval_(ref.set("Finished")))

        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, evaluator)
        startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv[IO](), None)
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
        Stream.eval_(ref.set("Started")) ++
          Stream("foo") ++
          Stream(" ") ++ // chunk delimiter
          Stream.sleep_(WorkTime) ++
          Stream("bar") ++
          Stream.eval_(ref.set("Finished"))

      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        (destination, filesystem) <- RefDestination()
        ref <- SignallingRef[IO, String]("Not started")
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ => testStream(ref)))
        startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv[IO](), None)
        _ <- latchGet(ref, "Started")
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
          Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2")))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv[IO], None)
        _ <- latchGet(sync, "Started")
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(Status.Running(_))) => ok
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
          Stream.eval(sync.set("Started")).as("1") ++ awaitS ++ Stream.eval(sync.set("Finished")).as("2")))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv[IO], None)
        _ <- latchGet(sync, "Started")
        _ <- push.cancel(TableId)
        _ <- await
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(Status.Canceled(_))) => ok
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
          Stream.eval_(sync.set("Started")) ++ awaitS ++ Stream.eval_(sync.set("Finished"))))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv[IO], None)
        _ <- latchGet(sync, "Finished")
        _ <- await // wait for concurrent update of push status
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(Status.Finished(_, _))) => ok
        }
      }
    }

    "retrieves the status of an errored push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())
      val ex = new Exception("boom")

      for {
        (destination, filesystem) <- RefDestination()
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, mkEvaluator(_ =>
          Stream.raiseError[IO](ex)))
        _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv[IO], None)
        _ <- await // wait for error handling
        pushStatus <- push.status(TableId)
        _ <- cleanup
      } yield {
        pushStatus must beLike {
          case \/-(Some(Status.Failed(ex, _, _))) => ex.getMessage must equal("boom")
        }
      }
    }

    "fails with ResultPushError.TableNotFound for unknown tables" >>* {
      for {
        (jm, cleanup) <- JobManager[IO, Int, Nothing]().compile.resource.lastOrError.allocated
        push <- mkResultPush(Map(), Map(), jm, mkEvaluator(_ => Stream.empty))
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
        push <- mkResultPush(Map(TableId -> testTable), Map(), jm, mkEvaluator(_ => Stream.empty))
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
          mkEvaluator(_ => Stream.eval(sync.set("Started")).as("1") ++ awaitS.as("2") ++ Stream.eval(sync.set("Finished")).as("3")))
        firstStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv[IO], None)
        secondStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv[IO], None)
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
            Stream.eval_(refFoo.set("Started")) ++ awaitS ++ Stream.eval_(refFoo.set("Finished"))
          case "queryBar" =>
            Stream.eval_(refBar.set("Started")) ++ awaitS ++ Stream.eval_(refBar.set("Finished"))
        })
        _ <- push.start(1, DestinationId, path1, ResultType.Csv[IO](), None)
        _ <- push.start(2, DestinationId, path2, ResultType.Csv[IO](), None)
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
        push <- mkResultPush(Map(TableId -> testTable), Map(), jm, mkEvaluator(_ => Stream.empty))
        pushRes <- push.start(TableId, UnknownDestinationId, ResourcePath.root(), ResultType.Csv[IO](), None)
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
        push <- mkResultPush(Map(), Map(DestinationId -> destination), jm, mkEvaluator(_ => Stream.empty))
        pushRes <- push.start(UnknownTableId, DestinationId, ResourcePath.root(), ResultType.Csv[IO](), None)
        _ <- cleanup
      } yield {
        pushRes must beAbnormal(ResultPushError.TableNotFound(UnknownTableId))
      }
    }
  }
}
