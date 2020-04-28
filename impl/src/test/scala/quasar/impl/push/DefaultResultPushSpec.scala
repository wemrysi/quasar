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
import cats.data.{Ior, NonEmptyList, NonEmptySet}
import cats.derived.auto.order._
import cats.effect.{Blocker, IO, Resource}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, text}
import fs2.concurrent.{Enqueue, Queue}

import java.lang.Integer
import java.time.{Instant, ZoneOffset}

import monocle.Prism

import org.mapdb.{DBMaker, Serializer}
import org.mapdb.serializer.GroupSerializer

import monocle.Traversal

import org.specs2.execute.AsResult
import org.specs2.specification.core.Fragment

import quasar.{ConditionMatchers, EffectfulQSpec}
import quasar.api.{Column, ColumnType, Label, Labeled, QueryEvaluator}
import quasar.api.destination._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.connector.{DataEvent, Offset}
import quasar.connector.destination._
import quasar.connector.render._
import quasar.impl.storage.RefIndexedStore
import quasar.impl.storage.mapdb._

import shims.{applicativeToCats, eqToScalaz, orderToCats, orderToScalaz, showToCats, showToScalaz}

import scala.Predef.classOf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scalaz.IMap

import shapeless._

import skolems.{∃, Forall}

import spire.math.Real

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  import ResultPushError._

  addSections

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

  final class QDestination(q: Enqueue[IO, Option[Filesystem]], support: QDestination.Support)
      extends Destination[IO] {

    import QDestination._

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

    def sinks = support match {
      case Create => NonEmptyList.one(createSink)
      case Upsert => NonEmptyList.one(upsertSink)
      case All => NonEmptyList.of(createSink, upsertSink)
    }

    val createSink: ResultSink[IO, Type] = ResultSink.create[IO, Type](RenderConfig.Csv()) {
      case (dst, _, bytes) =>
        bytes.through(text.utf8Decode)
          .evalMap(s => q.enqueue1(Some(Map(dst -> s))))
          .onFinalize(q.enqueue1(None))
    }

    val upsertSink: ResultSink[IO, Type] = {
      val consume = Forall[λ[α => ResultSink.UpsertSink.Args[IO, Type, α] => Stream[IO, OffsetKey.Actual[α]]]] { args =>
        args.input
          .flatMap {
            case DataEvent.Create(c) =>
              Stream.chunk(c)
                .through(text.utf8Decode)
                .evalMap(s => q.enqueue1(Some(Map(args.path -> s))))
                .drain

            case DataEvent.Commit(k) =>
              Stream.emit(k)

            case _ =>
              Stream.empty
          }
          .onFinalize(q.enqueue1(None))
      }

      ResultSink.upsert[IO, Type](RenderConfig.Csv())(consume)
    }
  }

  object QDestination {
    sealed trait Support
    object Create extends Support
    object Upsert extends Support
    object All extends Support

    def apply(supports: Support = All): IO[(QDestination, Stream[IO, Filesystem])] =
      Queue.noneTerminated[IO, Filesystem] map { q =>
        (new QDestination(q, supports), q.dequeue.scanMonoid)
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
        : Stream[IO, DataEvent[Any, OffsetKey.Actual[A]]] = {
      val creates =
        Stream(input.value)
          .through(text.utf8Encode)
          .chunks
          .map(DataEvent.Create(_))

      offsetColumn.tpe match {
        case OffsetKey.RealKey(_) =>
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.real(100)))

        case OffsetKey.StringKey(_) =>
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.string("id-100")))

        case OffsetKey.DateTimeKey(_) =>
          val epoch = Instant.EPOCH.atOffset(ZoneOffset.UTC)
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.dateTime(epoch)))
      }
    }
  }

  def mkEvaluator(f: PartialFunction[(String, Option[Offset]), IO[Stream[IO, String]]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    QueryEvaluator(f).mapF(Resource.liftF(_))

  def constEvaluator(s: IO[Stream[IO, String]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    QueryEvaluator(_ => Resource.liftF(s))

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

      pushes <- Resource liftF {
        MapDbPrefixStore[IO](
          "default-result-push-spec",
          db,
          Serializer.INTEGER :: (ResourcePathSerializer: GroupSerializer[ResourcePath]) :: HNil,
          Serializer.ELSA.asInstanceOf[GroupSerializer[∃[Push[?, String]]]],
          Blocker.liftExecutionContext(global))
      }

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

  val outputColumns: Traversal[∃[PushConfig[?, String]], PushConfig.OutputColumn] =
    new Traversal[∃[PushConfig[?, String]], PushConfig.OutputColumn] {
      def modifyF[F[_]: scalaz.Applicative](
          f: PushConfig.OutputColumn => F[PushConfig.OutputColumn])(
          s: ∃[PushConfig[?, String]]) = {

        val c: PushConfig[_, String] = s.value

        c match {
          case x @ PushConfig.Full(p, q, cs) =>
            cs.traverse(f).map(xs => ∃[PushConfig[?, String]](x.copy(columns = xs)))

          case x @ PushConfig.Incremental(_, _, cs, _, _) =>
            cs.traverse(f).map(xs => ∃[PushConfig[?, String]](x.copy(outputColumns = xs)))
        }
      }
    }

  val colX = NonEmptyList.one(Column("X", (ColumnType.Boolean, SelectedType(TypeIndex(0), None))))

  val resumePos =
    ResumeConfig(
      Column("id", (IdType.StringId, SelectedType(TypeIndex(1), Some(∃(Actual.integer(10)))))),
      Column("pos", OffsetKey.Formal.real(())),
      NonEmptyList.one(Left("pos")))

  def full(path: ResourcePath, q: String, cols: PushConfig.Columns = colX): ∃[PushConfig[?, String]] =
    ∃[PushConfig[?, String]](PushConfig.Full(path, q, cols))

  def incremental(
      path: ResourcePath,
      q: String,
      cols: List[PushConfig.OutputColumn] = colX.toList,
      resume: ResumeConfig[Real] = resumePos,
      initial: Option[OffsetKey.Actual[Real]] = None)
      : ∃[PushConfig[?, String]] =
    ∃[PushConfig[?, String]](PushConfig.Incremental(path, q, cols, resume, initial))

  def forallConfigs[A: AsResult](f: ∃[PushConfig[?, String]] => IO[A]): Fragment = {
    "full" >>* f(full(ResourcePath.root() / ResourceName("full"), "fullq"))
    "incremental" >>* f(incremental(ResourcePath.root() / ResourceName("incremental"), "incrementalq"))
  }

  "start" >> {
    "asynchronously pushes results to a destination" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            result <- await(started.sequence)

            filesystemAfterPush <- awaitFs(filesystem)
          } yield {
            filesystemAfterPush.keySet must equal(Set(config.value.path))
            filesystemAfterPush(config.value.path) must equal(dataString)
            result must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "rejects when full push running for same path" >> forallConfigs { config =>
      val path = config.value.path
      val fullCfg = full(path, "conflictq")

      for {
        (destination, _) <- QDestination()
        (_, data) <- controlledStream

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, fullCfg, None)
            secondStartStatus <- rp.start(DestinationId, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beLeft(NonEmptyList.one(PushAlreadyRunning(DestinationId, path)))
          }
        }
      } yield r
    }

    "rejects when incremental push running for same path" >> forallConfigs { config =>
      val path = config.value.path
      val incCfg = incremental(path, "conflictq")

      for {
        (destination, _) <- QDestination()
        (_, data) <- controlledStream

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, incCfg, None)
            secondStartStatus <- rp.start(DestinationId, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beLeft(NonEmptyList.one(PushAlreadyRunning(DestinationId, path)))
          }
        }
      } yield r
    }

    "allows concurrent push to a path in multiple destinations" >> forallConfigs { config =>
      val dest2 = new Integer(DestinationId.intValue + 1)

      for {
        (destination, _) <- QDestination()

        eval = constEvaluator(controlledStream.map(_._2))

        r <- mkResultPush(Map(DestinationId -> destination, dest2 -> destination), eval) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, config, None)
            secondStartStatus <- rp.start(dest2, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beRight
          }
        }
      } yield r
    }

    "fails with 'destination not found' when destination doesn't exist" >> forallConfigs { config =>
      mkResultPush(Map(), emptyEvaluator) use { rp =>
        rp.start(DestinationId, config, None)
          .map(_ must beLeft(NonEmptyList.one(DestinationNotFound(DestinationId))))
      }
    }

    "fails when selected type isn't found" >> forallConfigs { config =>
      val idx = TypeIndex(-1)
      val bad = Column("A", (ColumnType.Boolean, SelectedType(idx, None)))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          rp.start(DestinationId, cfg, None)
        }
      } yield {
        startStatus must beLeft(NonEmptyList.one(TypeNotFound(DestinationId, "A", idx)))
      }
    }

    "fails when a required type argument is missing" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val bad = Column("A", (ColumnType.String, SelectedType(idx, None)))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
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

    "fails when wrong kind of type argument given" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val bad = Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.boolean(false))))))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
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

    "fails when integer type argument is out of bounds" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val low = Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.integer(0))))))
      val high = Column("A", (ColumnType.String, SelectedType(idx, Some(∃(Actual.integer(300))))))

      val cfgL = outputColumns.set(low)(config)
      val cfgH = outputColumns.set(high)(config)

      for {
        (dest, _) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          for {
            startLow <- rp.start(DestinationId, cfgL, None)
            startHigh <- rp.start(DestinationId, cfgH, None)
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

    "fails when enum type argument selection is not found" >> forallConfigs { config =>
      val idx = TypeIndex(2)
      val bad = Column("B", (ColumnType.Number, SelectedType(idx, Some(∃(Actual.enumSelect("32-bits"))))))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
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

    "fails when column type not a valid coercion of corresponding scalar type" >> todo

    "fails when destination doesn't support full push" >> todo

    "fails when destination doesn't support incremental push" >> todo

    "replaces a previous full push to path" >> todo

    "replaces a previous incremental push to path" >> todo
  }

  "update" >> {
    "restarts previous full push using saved query" >> todo
    "resumes previous incremental push from saved offset" >> todo
    "resumes previous incremental push from beginning when no offset" >> todo
    "fails if already running" >> todo
  }

  "cancel" >> {
    "halts a running initial push" >> forallConfigs { config =>
      val path = config.value.path

      for {
        (destination, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)

            filesystemAfterPush <- awaitFs(filesystem, 1)

            cancelRes <- rp.cancel(DestinationId, path)

            terminal <- await(startRes.sequence)
          } yield {
            filesystemAfterPush.keySet must equal(Set(path))
            // check if a *partial* result was pushed
            filesystemAfterPush(path) must equal(W1)
            cancelRes must beNormal
            terminal must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "halts a running update push" >> todo

    "no-op when push already completed" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- awaitFs(filesystem)
            _ <- await(startRes.sequence)

            cancelRes <- rp.cancel(DestinationId, config.value.path)
          } yield cancelRes must beNormal
        }
      } yield r
    }

    "no-op when no push to path" >>* {
      for {
        (destination, _) <- QDestination()

        cancelRes <-
          mkResultPush(Map(DestinationId -> destination), emptyEvaluator)
            .use(_.cancel(DestinationId, ResourcePath.root() / ResourceName("out")))
      } yield {
        cancelRes must beNormal
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      mkResultPush(Map(), emptyEvaluator)
        .use(_.cancel(DestinationId, ResourcePath.root()))
        .map(_ must beAbnormal(DestinationNotFound(DestinationId)))
    }
  }

  "pushedTo" >> {
    def lookupPushed(
        r: Either[DestinationNotFound[Integer], Map[ResourcePath, ∃[Push[?, String]]]],
        p: ResourcePath)
        : Option[Push[_, String]] =
      r.toOption.flatMap(_.get(p)).map(_.value)

    "empty when nothing has been pushed" >>* {
      for {
        (destination, _) <- QDestination()

        pushes <-
          mkResultPush(Map(DestinationId -> destination), emptyEvaluator)
            .use(_.pushedTo(DestinationId))
      } yield {
        pushes must beLike {
          case Right(m) => m.isEmpty must beTrue
        }
      }
    }

    "includes running push" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            _ <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitFs(filesystem, 1)

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Running(_, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes running full push when previous status exists" >> todo

    "includes canceled push" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitFs(filesystem, 1)

            _ <- rp.cancel(DestinationId, config.value.path)

            _ <- await(startRes.sequence)

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Canceled(_, _, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes completed push" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Finished(_, _, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes pushes that failed to initialize along with error that led to failure" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        eval = constEvaluator(IO.raiseError[Stream[IO, String]](new Exception("boom")))

        pushed <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)
            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Failed(_, _, _, msg)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
            msg must contain("boom")
        }
      }
    }

    "includes pushes that failed during streaming along with the error that led to falure" >> forallConfigs { config =>
      val ex = new Exception("errored!")

      for {
        (destination, filesystem) <- QDestination()

        eval = constEvaluator(IO(dataStream ++ Stream.raiseError[IO](ex)))

        pushed <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)
            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Failed(_, _, _, msg)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
            msg must contain("errored!")
        }
      }
    }

    "results in previous terminal status if canceled before started" >> todo

    "results in unknown status if interrupted/crash while running" >> todo

    "returns 'destination not found' when destination doesn't exist" >>* {
      mkResultPush(Map(), emptyEvaluator)
        .use(_.pushedTo(DestinationId))
        .map(_ must beLeft(DestinationNotFound(DestinationId)))
    }
  }

  "cancel all" >> {
    "aborts all running pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val fullCfg = full(path1, "queryFoo")

      val path2 = ResourcePath.root() / ResourceName("bar")
      val incCfg = incremental(path2, "queryBar")

      for {
        (destination, filesystem) <- QDestination()

        (ctlA, dataA) <- controlledStream
        (ctlB, dataB) <- controlledStream

        eval = mkEvaluator {
          case ("queryFoo", _) => IO(dataA)
          case ("queryBar", _) => IO(dataB)
        }

        r <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            s1 <- rp.start(DestinationId, fullCfg, None)
            s2 <- rp.start(DestinationId, incCfg, None)

            _ <- ctlA.emit(W1)
            _ <- ctlB.emit(W2)

            _ <- awaitFs(filesystem, 2)

            _ <- rp.cancelAll

            r1 <- await(s1.sequence)
            r2 <- await(s2.sequence)
          } yield {
            r1 must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }

            r2 must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "no-op when no running pushes" >>* {
      mkResultPush(Map(), emptyEvaluator).use(_.cancelAll).as(ok)
    }
  }
}
