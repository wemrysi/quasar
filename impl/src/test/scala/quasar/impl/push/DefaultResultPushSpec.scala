/*
 * Copyright 2020 Precog Data
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

import cats._
import cats.data.{Ior, NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.derived.auto.order._
import cats.effect.{Blocker, IO, Resource}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, text}
import fs2.concurrent.{Enqueue, Queue}

import java.lang.Integer
import java.time.Instant

import monocle.Prism

import org.mapdb.{DBMaker, Serializer}
import org.mapdb.serializer.GroupSerializer

import quasar.{Condition, ConditionMatchers, EffectfulQSpec}
import quasar.api.{Column, ColumnType, Label, Labeled, QueryEvaluator}
import quasar.api.destination._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.connector.{DataEvent, Offset}
import quasar.connector.destination._
import quasar.connector.render._
import quasar.impl.storage.{PrefixStore, RefIndexedStore}
import quasar.impl.storage.mapdb._

import shims.{eqToScalaz, orderToCats, orderToScalaz, showToCats, showToScalaz}

import scala.Predef.classOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scalaz.IMap

import shapeless._

import skolems.∃

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  import ResultPushError._

  implicit val tmr = IO.timer(global)

  val Timeout = 10.seconds

  implicit val jintegerOrder: Order[Integer] =
    Order.by(_.intValue)

  implicit val jintegershow: Show[Integer] =
    Show.fromToString

  val DestinationId: Integer = new Integer(43)
  val QDestinationType = DestinationType("ref", 1L)

  type Filesystem = Map[ResourcePath, String]

  // NB: These must be at least 3 bytes each to ensure they're
  //     emitted from utf8Decode without additional input.
  val W1 = "lorum"
  val W2 = "ipsum"
  val W3 = "dolor"

  // The string formed by concatentating Ws
  val dataString = W1 + W2 + W3

  final class QDestination(q: Enqueue[IO, Option[Filesystem]]) extends Destination[IO] {
    sealed trait Constructor[P] extends ConstructorLike[P] with Product with Serializable

    sealed trait Type extends Product with Serializable

    case object Bool extends Type

    case class Varchar(length: Int) extends Type
    case object Varchar extends Constructor[Int]

    case class Num(width: Int) extends Type
    case object Num extends Constructor[Int]

    sealed trait TypeId extends Product with Serializable
    case object BoolId extends TypeId
    case object VarcharId extends TypeId
    case object NumId extends TypeId

    val typeIdOrdinal =
      Prism.partial[Int, TypeId] {
        case 0 => BoolId
        case 1 => VarcharId
        case 2 => NumId
      } {
        case BoolId => 0
        case VarcharId => 1
        case NumId => 2
      }

    val typeIdLabel =
      Label label {
        case BoolId => "BOOLEAN"
        case VarcharId => "VARCHAR"
        case NumId => "NUMBER"
      }

    def coerce(s: ColumnType.Scalar): TypeCoercion[TypeId] =
      s match {
        case ColumnType.Boolean => TypeCoercion.Satisfied(NonEmptyList.one(BoolId))
        case ColumnType.String => TypeCoercion.Satisfied(NonEmptyList.one(VarcharId))
        case ColumnType.Number => TypeCoercion.Satisfied(NonEmptyList.one(NumId))
        case other => TypeCoercion.Unsatisfied(Nil, None)
      }

    def construct(id: TypeId): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]] =
      id match {
        case BoolId =>
          Left(Bool)

        case VarcharId =>
          Right(formalConstructor(
            Varchar,
            "Length",
            Formal.integer(Some(Ior.both(1, 256)), None)))

        case NumId =>
          Right(formalConstructor(
            Num,
            "Width",
            Formal.enum[Int](
              "4-bits" -> 4,
              "8-bits" -> 8,
              "16-bits" -> 16)))
      }

    def destinationType: DestinationType = QDestinationType

    // TODO: optional incremental sink
    def sinks = NonEmptyList.one(csvSink)

    val csvSink: ResultSink[IO, Type] = ResultSink.create[IO, Type](RenderConfig.Csv()) {
      case (dst, _, bytes) =>
        bytes.through(text.utf8Decode)
          .evalMap(s => q.enqueue1(Some(Map(dst -> s))))
          .onFinalize(q.enqueue1(None))
    }
  }

  object QDestination {
    def apply(): IO[(QDestination, Stream[IO, Filesystem])] =
      Queue.noneTerminated[IO, Filesystem] map { q =>
        (new QDestination(q), q.dequeue.scanMonoid)
      }
  }

  final class MockResultRender extends ResultRender[IO, String] {
    def render(
        input: String,
        columns: NonEmptyList[Column[ColumnType.Scalar]],
        config: RenderConfig,
        rowLimit: Option[Long])
        : Stream[IO, Byte] =
      Stream(input).through(text.utf8Encode)

    def renderUpserts[A](
        input: RenderInput[String],
        idColumn: Column[IdType],
        offsetColumn: Column[OffsetKey.Formal[Unit, A]],
        renderedColumns: NonEmptyList[Column[ColumnType.Scalar]],
        config: RenderConfig.Csv,
        limit: Option[Long])
        : Stream[IO, DataEvent[Any, OffsetKey.Actual[A]]] =
      ???
  }

  def mkEvaluator(fn: (String, Option[Offset]) => IO[Stream[IO, String]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    QueryEvaluator(fn.tupled).mapF(Resource.liftF(_))

  def constEvaluator(s: IO[Stream[IO, String]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    mkEvaluator((_, _) => s)

  val emptyEvaluator: QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    constEvaluator(IO(Stream()))

  trait StreamControl[F[_]] {
    // Cause the stream to emit a value
    def emit(s: String): F[Unit]

    // Forcibly halts the stream
    def halt: F[Unit]

    // Completes when the stream has halted for any reason
    def halted: F[Unit]
  }

  val controlledStream: IO[(StreamControl[IO], Stream[IO, String])] =
    Queue.noneTerminated[IO, String] flatMap { q =>
      Deferred[IO, Unit] map { d =>
        val toutErr = new RuntimeException(s"Expected data stream to terminate within ${Timeout} millis.")

        val s = q.dequeue.onFinalize(d.complete(()))

        val ctl = new StreamControl[IO] {
          def emit(v: String) = q.enqueue1(Some(v))
          val halt = q.enqueue1(None)
          val halted = d.get.timeoutTo(Timeout, IO.raiseError(toutErr))
        }

        (ctl, s)
      }
    }

  val dataStream: Stream[IO, String] =
    Stream.fixedDelay[IO](100.millis).zipRight(Stream(W1, W2, W3))

  def mkResultPush(
      destinations: Map[Integer, Destination[IO]],
      evaluator: QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]])
      : Resource[IO, ResultPush[IO, Integer, String]] = {

    val lookupDestination: Integer => IO[Option[Destination[IO]]] =
      destinationId => IO(destinations.get(destinationId))

    val render = new MockResultRender

    for {
      db <- Resource.make(IO(DBMaker.memoryDB().make()))(db => IO(db.close()))

      pushes0 <- Resource liftF {
        MapDbPrefixStore[IO](
          "default-result-push-spec",
          db,
          Serializer.INTEGER :: (ResourcePathSerializer: GroupSerializer[ResourcePath]) :: HNil,
          Serializer.ELSA.asInstanceOf[GroupSerializer[Push[_, String]]],
          Blocker.liftExecutionContext(global))
      }

      // skolems.Exists isn't Serializable
      pushes = PrefixStore.xmapValueF(pushes0)(
        p => IO(∃[Push[?, String]](p)))(
        p => IO(p.value: Push[_, String]))

      ref <- Resource.liftF(Ref[IO].of(IMap.empty[Integer :: ResourcePath :: HNil, ∃[OffsetKey.Actual]]))

      offsets = RefIndexedStore(ref)

      resultPush <-
        DefaultResultPush[IO, Integer, String, String](
          10,
          lookupDestination,
          evaluator,
          render,
          pushes,
          offsets)

    } yield resultPush
  }

  def awaitFs(fss: Stream[IO, Filesystem], count: Long = -1): IO[Filesystem] = {
    val (s, msg) =
      if (count <= 0)
        (fss, "final filesystem")
      else
        (fss.take(count + 1), s"${count} filesystem updates")

    s.compile.lastOrError.timeoutTo(
      Timeout,
      IO.raiseError(new RuntimeException(s"Expected $msg within ${Timeout}.")))
  }

  def await[A](io: IO[A]): IO[A] =
    io.timeoutTo(Timeout, IO.raiseError[A](new RuntimeException(s"Expected completion within ${Timeout}.")))

  def awaitStatusLike(fs: IO[Status.Terminal])(f: PartialFunction[Status.Terminal, _])
      : IO[Status.Terminal] = {

    fs.flatMap(s =>
        if (f.isDefinedAt(s))
          IO.pure(s)
        else
          IO.raiseError(new RuntimeException(s"Unexpected status: ${s.show}")))
      .timeoutTo(
        Timeout,
        IO.raiseError(new RuntimeException(s"Expected a matching `Status.Terminal` within $Timeout.")))
  }

  val colX = NonEmptyList.one(Column("X", (ColumnType.Boolean, SelectedType(TypeIndex(0), None))))

  def full(path: ResourcePath, q: String, cols: PushConfig.Columns = colX): ∃[PushConfig[?, String]] =
    ∃[PushConfig[?, String]](PushConfig.Full(path, q, cols))

  "start" >> {
    "asynchronously pushes results to a destination" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"

      for {
        (destination, filesystem) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            started <- rp.start(DestinationId, full(pushPath, query), None)

            result <- await(started.sequence)

            filesystemAfterPush <- awaitFs(filesystem)
          } yield {
            filesystemAfterPush.keySet must equal(Set(pushPath))
            filesystemAfterPush(pushPath) must equal(dataString)
            result must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "rejects an already running push of a path to the same destination" >>* {
      val path = ResourcePath.root()

      for {
        (destination, _) <- QDestination()
        (_, data) <- controlledStream

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, full(path, "q"), None)
            secondStartStatus <- rp.start(DestinationId, full(path, "q2"), None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beLeft(NonEmptyList.one(PushAlreadyRunning(DestinationId, path)))
          }
        }
      } yield r
    }

    "allows concurrent push to a path in multiple destinations" >>* {
      val path = ResourcePath.root()
      val dest2 = new Integer(DestinationId.intValue + 1)

      for {
        (destination, _) <- QDestination()

        eval = constEvaluator(controlledStream.map(_._2))

        r <- mkResultPush(Map(DestinationId -> destination, dest2 -> destination), eval) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, full(path, "q1"), None)
            secondStartStatus <- rp.start(dest2, full(path, "q2"), None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beRight
          }
        }
      } yield r
    }

    "fails with 'destination not found' when destination doesn't exist" >>* {
      val path = ResourcePath.root()

      mkResultPush(Map(), emptyEvaluator) use { rp =>
        rp.start(DestinationId, full(path, "q"), None)
          .map(_ must beLeft(NonEmptyList.one(DestinationNotFound(DestinationId))))
      }
    }

    "fails when selected type isn't found" >>* {
      val path = ResourcePath.root()
      val idx = TypeIndex(-1)
      val cols = NonEmptyList.one(Column("A", (ColumnType.Boolean, SelectedType(idx, None))))

      for {
        (dest, _) <- QDestination()

        startStatus <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          rp.start(DestinationId, full(path, "q", cols), None)
        }
      } yield {
        startStatus must beLeft(NonEmptyList.one(TypeNotFound(DestinationId, "A", idx)))
      }
    }

    "fails when a required type argument is missing" >>* {
      val path = ResourcePath.root()
      val idx = TypeIndex(1)
      val cols = NonEmptyList.one(Column("A", (ColumnType.String, SelectedType(idx, None))))

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, full(path, "q", cols), None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(ResultPushError.TypeConstructionFailed(
            DestinationId,
            "A",
            "VARCHAR",
            NonEmptyList(ParamError.ParamMissing("Length", _), Nil)), Nil) => ok
        }
      }
    }

    "fails when wrong kind of type argument given" >>* {
      val path = ResourcePath.root()
      val idx = TypeIndex(1)
      val cols = NonEmptyList.one(Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.boolean(false)))))))

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, full(path, "q", cols), None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(ResultPushError.TypeConstructionFailed(
            DestinationId,
            "A",
            "VARCHAR",
            NonEmptyList(ParamError.ParamMismatch("Length", _, _), Nil)), Nil) => ok
        }
      }
    }

    "fails when integer type argument is out of bounds" >>* {
      val path = ResourcePath.root()
      val idx = TypeIndex(1)
      val colsLow = NonEmptyList.one(Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.integer(0)))))))
      val colsHigh = NonEmptyList.one(Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.integer(300)))))))

      for {
        (dest, _) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          for {
            startLow <- rp.start(DestinationId, full(path, "l", colsLow), None)
            startHigh <- rp.start(DestinationId, full(path, "h", colsHigh), None)
          } yield {
            startLow must beLeft.like {
              case NonEmptyList(ResultPushError.TypeConstructionFailed(
                DestinationId,
                "A",
                "VARCHAR",
                NonEmptyList(ParamError.IntOutOfBounds("Length", 0, Ior.Both(1, 256)), Nil)), Nil) => ok
            }

            startHigh must beLeft.like {
              case NonEmptyList(ResultPushError.TypeConstructionFailed(
                DestinationId,
                "A",
                "VARCHAR",
                NonEmptyList(ParamError.IntOutOfBounds("Length", 300, Ior.Both(1, 256)), Nil)), Nil) => ok
            }
          }
        }
      } yield r
    }

    "fails when enum type argument selection is not found" >>* {
      val path = ResourcePath.root()
      val idx = TypeIndex(2)
      val cols = NonEmptyList.one(Column("B", (ColumnType.Number, SelectedType(idx, Some(∃(Actual.enumSelect("32-bits")))))))

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, full(path, "q", cols), None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(ResultPushError.TypeConstructionFailed(
            DestinationId,
            "B",
            "NUMBER",
            NonEmptyList(ParamError.ValueNotInEnum("Width", "32-bits", xs), Nil)), Nil) =>
              xs must_=== NonEmptySet.of("4-bits", "8-bits", "16-bits")
        }
      }
    }

    "fails when column type not a valid coercion of corresponding scalar type" >> ko

    "start an incremental when previous was full" >> ko
    "start an incremental when previous was incremental" >> ko
    "start a full when prevous was incremental" >> ko
  }
/*
  "update" >> {
    "restarts previuos full push using saved query" >> ko
    "resumes previous incremental push from saved offset" >> ko
    "fails if already running" >> ko
  }

  "cancel" >> {
    "aborts a running full push" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          startRes <- push.start(TableId, colX, DestinationId, pushPath, ResultType.Csv, None)

          _ <- ctl.emit(W1)

          filesystemAfterPush <- awaitFs(filesystem, 1)

          cancelRes <- push.cancel(TableId, DestinationId)

          _ <- await(ctl.halted)
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPath))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPath) must equal(W1)
          startRes must beNormal
          cancelRes must beNormal
        }
      }
    }

    // check that state is preserved
    "aborts a running incremental push" >> ko

    "no-op when push already completed" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          startRes <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- awaitFs(filesystem)
          _ <- await(halted)

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          startRes must beNormal
          cancelRes must beNormal
        }
      }
    }

    "no-op when no push for extant table" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beNormal
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }

    "returns 'table not found' when table doesn't exist" >>* {
      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.TableNotFound(TableId))
        }
      }
    }
  }

  "pushesTo" >> {
    "empty when nothing has been pushed" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))

          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLike {
            case Right(m) => m.isEmpty must beTrue
          }
        }
      }
    }

    "includes running push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- ctl.emit(W1)
          _ <- awaitFs(filesystem, 1)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Running(_)) => true
          }
        } yield ok
      }
    }

    "includes canceled push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- ctl.emit(W1)
          _ <- awaitFs(filesystem, 1)

          _ <- push.cancel(TableId, DestinationId)

          _ <- await(ctl.halted)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Canceled(_, _)) => true
          }
        } yield ok
      }
    }

    "includes completed push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- await(halted)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Finished(_, _)) => true
          }
        } yield ok
      }
    }

    "includes pushes that failed to initialize along with error that led to failure" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO.raiseError[Stream[IO, String]](new Exception("boom"))))

          pushStatus <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          status <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, _, _, Status.Failed(_, _, _)) => true
          }
        } yield {
          status must beLike {
            case PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "includes pushes that failed during streaming along with the error that led to falure" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())
      val ex = new Exception("boom")

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data ++ Stream.raiseError[IO](ex))))

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- await(halted)

          status <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, _, _, Status.Failed(_, _, _)) => true
          }
        } yield {
          status must beLike {
            case PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLeft(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }
  }

  "cancel all" >> {
    "aborts all running pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

      val path2 = ResourcePath.root() / ResourceName("bar")
      val testTable2 = TableRef(TableName("bar"), "queryBar", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctlA, dataA) <- controlledStream
          (ctlB, dataB) <- controlledStream

          push <- mkResultPush(
            Map(1 -> testTable1, 2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryFoo" => IO(dataA)
              case "queryBar" => IO(dataB)
            })

          _ <- push.start(1, colX, DestinationId, path1, ResultType.Csv, None)
          _ <- push.start(2, colX, DestinationId, path2, ResultType.Csv, None)

          _ <- ctlA.emit(W1)
          _ <- ctlB.emit(W2)

          _ <- awaitFs(filesystem, 2)

          _ <- push.cancelAll

          _ <- await(ctlA.halted)
          _ <- await(ctlB.halted)
        } yield ok
      }
    }

    "no-op when no running pushes" >>* {
      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          _ <- push.cancelAll
        } yield ok
      }
    }
  }
*/
}
