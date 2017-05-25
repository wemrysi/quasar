/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.mimir

import slamdata.Predef._
import quasar._
import quasar.blueeyes.json.{JNum, JValue}
import quasar.common._
import quasar.connector._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import quasar.precog.common.{Path, RValue}
import quasar.precog.common.security.APIKey
import quasar.precog.util.IOUtils
import quasar.yggdrasil.PathMetadata
import quasar.yggdrasil.vfs.ResourceError
import quasar.yggdrasil.bytecode.JType

import fs2.{Handle, Pull, Stream}
import fs2.async.mutable.Queue
import fs2.interop.scalaz._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

import org.slf4s.Logging

import pathy.Path._

import delorean._

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Random

object Mimir extends BackendModule with Logging {

  // pessimisticaly equal to couchbase's
  type QS[T[_[_]]] =
    QScriptCore[T, ?] :\:
    EquiJoin[T, ?] :/:
    Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  type Repr = Precog.Table
  type M[A] = Task[A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def MonadFsErrM = ???
  def PhaseResultTellM = ???
  def PhaseResultListenM = ???
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = Unit

  def parseConfig(uri: ConnectionUri): FileSystemDef.DefErrT[Task, Config] =
    ().point[FileSystemDef.DefErrT[Task, ?]]

  def compile(cfg: Config): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    ((reflNT[Task], ().point[Task])).point[FileSystemDef.DefErrT[Task, ?]]

  val Type = FileSystemType("mimir")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {

// M = Backend
// F[_] = MapFunc[T, ?]
// B = Repr
// A = SrcHole
// AlgebraM[M, CoEnv[A, F, ?], B] = AlgebraM[Backend, CoEnv[Hole, MapFunc[T, ?], ?], Repr]
//def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]): AlgebraM[M, CoEnv[A, F, ?], B]
// f.cataM(interpretM)

    lazy val planQST: AlgebraM[Backend, QScriptTotal[T, ?], Repr] =
      _.run.fold(
        planQScriptCore,
        _.run.fold(
          _ => ???,   // ProjectBucket
          _.run.fold(
            _ => ???,   // ThetaJoin
            _.run.fold(
              planEquiJoin,
              _.run.fold(
                _ => ???,    // ShiftedRead[ADir]
                _.run.fold(
                  planShiftedRead,
                  _.run.fold(
                    _ => ???,   // Read[ADir]
                    _.run.fold(
                      _ => ???,   // Read[AFile]
                      _ => ???))))))))    // DeadEnd

    lazy val planMapFunc: AlgebraM[Backend, MapFunc[T, ?], Repr] = {
      // EJson => Data => JValue => RValue => Table
      case MapFuncs.Constant(ejson) =>
        val data: Data = ejson.cata(Data.fromEJson)
        val jvalue: JValue = JValue.fromData(data)
        val rvalue: RValue = RValue.fromJValue(jvalue)
        Precog.Table.fromRValues(scala.Stream(rvalue)).point[Backend]
      case _ => ???
    }

    lazy val planQScriptCore: AlgebraM[Backend, QScriptCore[T, ?], Repr] = {
      case qscript.Map(src, f) => f.cataM(interpretM(κ(src.point[Backend]), planMapFunc))
      case qscript.LeftShift(src, struct, id, repair) => ???
      case qscript.Reduce(src, bucket, reducers, repair) => ???
      case qscript.Sort(src, bucket, order) => ???
      case qscript.Filter(src, f) => ???
      case qscript.Union(src, lBranch, rBranch) => ???
      case qscript.Subset(src, from, op, count) =>
        for {
          fromRepr <- from.cataM(interpretM(κ(src.point[Backend]), planQST))
          countRepr <- count.cataM(interpretM(κ(src.point[Backend]), planQST))
          back <- {
            def result = for {
              vals <- countRepr.toJson
              nums = vals collect { case n: JNum => n.toLong.toInt } // TODO error if we get something strange
              number = nums.head
              back <- op match {
                case Take => Future.successful(fromRepr.takeRange(0, number))
                case Drop => Future.successful(fromRepr.takeRange(number, slamdata.Predef.Int.MaxValue.toLong)) // blame precog
                case Sample => fromRepr.sample(number, List(Precog.trans.TransSpec1.Id)).map(_.head) // the number of Reprs returned equals the number of transspecs
              }
            } yield back

            result.toTask.liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]
          }
        } yield back

      case qscript.Unreferenced() => Precog.Table.empty.point[Backend]
    }

    lazy val planEquiJoin: AlgebraM[Backend, EquiJoin[T, ?], Repr] = _ => ???

    lazy val planShiftedRead: AlgebraM[Backend, Const[ShiftedRead[AFile], ?], Repr] = {
      case Const(ShiftedRead(path, status)) => {
        val pathStr: String = pathy.Path.posixCodec.printPath(path)
        val loaded: EitherT[Task, FileSystemError, Repr] =
          for {
            apiKey <- Precog.RootAPIKey.toTask.liftM[EitherT[?[_], FileSystemError, ?]]
            table <- Precog.Table.constString(Set(pathStr)).load(apiKey, JType.JUniverseT).mapT(_.toTask).leftMap(toFSError)
          } yield {
            status match {
              case IdOnly => table.transform(Precog.trans.constants.SourceKey.Single)
              case IncludeId => table
              case ExcludeId => table.transform(Precog.trans.constants.SourceValue.Single)
            }
          }

        // TODO duplicated in listContents
        val result: FileSystemErrT[Task, ?] ~> Backend =
          Hoist[FileSystemErrT].hoist[Task, PhaseResultT[Configured, ?]](
            liftMT[Configured, PhaseResultT] compose liftMT[Task, ConfiguredT])

        result.apply(loaded)
      }
    }

    def planQSM(in: QSM[T, Repr]): Backend[Repr] =
      in.run.fold(planQScriptCore, _.run.fold(planEquiJoin, planShiftedRead))

    cp.cataM(planQSM _)
  }

  private def dequeueStreamT[F[_]: Functor, A](q: Queue[F, A])(until: A => Boolean): StreamT[F, A] = {
    StreamT.unfoldM[F, A, Queue[F, A]](q) { q =>
      q.dequeue1 map { a =>
        if (until(a))
          None
        else
          Some((a, q))
      }
    }
  }

  private def dirToPath(dir: ADir): Path = Path(pathy.Path.posixCodec.printPath(dir))
  private def fileToPath(file: AFile): Path = Path(pathy.Path.posixCodec.printPath(file))

  private def toSegment: PathMetadata => PathSegment = {
    case PathMetadata(path, PathMetadata.DataDir(_)) =>
      DirName(path.components.mkString("/")).left[FileName]

    case PathMetadata(path, PathMetadata.DataOnly(_)) =>
      FileName(path.components.mkString("/")).right[DirName]

    case PathMetadata(path, PathMetadata.PathOnly) =>
      sys.error(s"found path $path")
  }

  private def children(dir: ADir): EitherT[Future, ResourceError, Set[PathSegment]] = {
    log.debug(s"children of $dir")
    Precog.showContents(dirToPath(dir)).map(_.map(toSegment))
  }

  private def toFSError: ResourceError => FileSystemError = {
    case ResourceError.Corrupt(msg) => ???
    case ResourceError.IOError(ex) => ???
    case ResourceError.IllegalWriteRequestError(msg) => ???
    case ResourceError.PermissionsError(msg) => ???
    case ResourceError.NotFound(msg) => ???
    case ResourceError.ResourceErrors(errs) => ???
  }

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Backend[AFile] = ???
    def evaluatePlan(repr: Repr): Backend[ResultHandle] = ???
    def more(h: ResultHandle): Backend[Vector[Data]] = ???
    def close(h: ResultHandle): Configured[Unit] = ???
    def explain(repr: Repr): Backend[String] = ???

    def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      val segments: FileSystemErrT[Future, Set[PathSegment]] =
        for {
          key <- Precog.RootAPIKey.liftM[FileSystemErrT]
          stuff <- children(dir).leftMap(toFSError)
        } yield stuff

      val futureToTask: FileSystemErrT[Future, ?] ~> FileSystemErrT[Task, ?] =
        Hoist[FileSystemErrT].hoist[Future, Task](λ[Future ~> Task](_.toTask))

      val result: FileSystemErrT[Task, ?] ~> Backend =
        Hoist[FileSystemErrT].hoist[Task, PhaseResultT[Configured, ?]](
          liftMT[Configured, PhaseResultT] compose liftMT[Task, ConfiguredT])

      result.apply(futureToTask.apply(segments))
    }

    def fileExists(file: AFile): Configured[Boolean] = {
      val dir: ADir = pathy.Path.fileParent(file)

      def res: Future[Boolean] = for {
        key <- Precog.RootAPIKey
        back <- children(dir).fold(_ => false, _.contains(pathy.Path.fileName(file).right))
      } yield back

      res.toTask.liftM[ConfiguredT]
    }
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = ???
    def read(h: ReadHandle): Backend[Vector[Data]] = ???
    def close(h: ReadHandle): Configured[Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    private val QueueLimit = 100

    private val map: ConcurrentHashMap[WriteHandle, Queue[Task, Vector[Data]]] =
      new ConcurrentHashMap

    private val cur = new AtomicLong(0L)

    def open(file: AFile): Backend[WriteHandle] = {
      val run = Task suspend {
        log.debug(s"open file $file")

        val id = cur.getAndIncrement()
        val handle = WriteHandle(file, id)

        for {
          queue <- Queue.bounded[Task, Vector[Data]](QueueLimit)
          _ <- Task.delay(log.debug(s"got a queue $queue"))

          path = fileToPath(file)
          jvs = dequeueStreamT(queue)(_.isEmpty).map(_.map(JValue.fromData))

          ingestion = Precog.ingest(path, jvs.trans(λ[Task ~> Future](_.unsafeToFuture))).toTask

          // run asynchronously forever
          _ <- Task.delay(ingestion.unsafePerformAsync(_ => ()))
          _ <- Task.delay(log.debug(s"started the ingest stuff"))

          _ <- Task.delay(map.put(handle, queue))
        } yield handle
      }

      run.liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]
    }

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      log.debug(s"write to $h and $chunk")

      if (chunk.isEmpty) {
        Vector.empty[FileSystemError].point[Configured]
      } else {
        val t = for {
          optQueue <- Task.delay(Option(map.get(h)))
          _ <- optQueue.map(_.enqueue1(chunk)).getOrElse(Task.now(()))
        } yield Vector.empty[FileSystemError]

        t.liftM[ConfiguredT]
      }
    }

    def close(h: WriteHandle): Configured[Unit] = {
      val t = for {
        optQueue <- Task.delay(Option(map.get(h)))

        _ <- Task.delay(log.debug(s"close $h"))

        _ <- optQueue.map(_.enqueue1(Vector.empty)).getOrElse(Task.now(()))
      } yield ()

      t.liftM[ConfiguredT]
    }
  }

  // move and delete assume nihdb semantics
  object ManageFileModule extends ManageFileModule {
    import java.io.File

    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      def inner(fpath: APath, dpath: APath): Task[Unit] = {
        val from = new File(Precog.Config.dataDir, pathy.Path.posixCodec.printPath(fpath))
        val dest = new File(Precog.Config.dataDir, pathy.Path.posixCodec.printPath(dpath))

        // yolo (for context, renameTo really doesn't work well)
        Task delay { from.renameTo(dest); () }
      }

      inner(scenario.src, scenario.dst).liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]
    }

    def delete(path: APath): Backend[Unit] = {
      val target = new File(Precog.Config.dataDir, pathy.Path.posixCodec.printPath(path))
      val t = Task delay { IOUtils.recursiveDelete(target).unsafePerformIO(); () }
      t.liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]
    }

    def tempFile(near: APath): Backend[AFile] = {
      for {
        seed <- Task.delay(Random.nextLong.toString).liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]

        // yolo
        target = parentDir(near).get </> file(seed)

        h <- WriteFileModule.open(target)
        _ <- WriteFileModule.write(h, Vector(Data.Obj())).liftM[PhaseResultT].liftM[FileSystemErrT]
        _ <- WriteFileModule.close(h).liftM[PhaseResultT].liftM[FileSystemErrT]
      } yield target
    }
  }
}
