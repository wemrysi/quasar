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

package quasar.fs

import slamdata.Predef._

import quasar.{Data, DataCodec}
import quasar.contrib.pathy.{prettyPrint, AFile, APath, PathSegment}
import quasar.contrib.scalaz.MonadState_
import quasar.fp.TaskRef
import quasar.fp.numeric.{Natural, Positive}
import quasar.fp.ski.κ

import java.io.{File => JFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{
  Files,
  FileVisitResult,
  Path => JPath,
  Paths,
  SimpleFileVisitor,
  StandardCopyOption,
  StandardOpenOption
}
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import pathy.Path.{refineType, DirName, FileName}
import scalaz.{~>, \/, -\/, \/-, EitherT, Monad, State}
import scalaz.Scalaz._
import scalaz.concurrent.Task

/** A local file system implementation.
  *
  * The implementations are not guaranteed to be atomic.
  */
class Local private (baseDir: JFile) {

  case class ReadState(
    current: Long,
    handles: Map[Long, (AtomicBoolean, Natural, Option[Positive])])

  case class WriteState(
    current: Long,
    handles: Set[Long])

  def readFile[F[_]: Monad](implicit F: MonadState_[F, ReadState]) = λ[ReadFile ~> F] {
    case ReadFile.Open(file, offset, limit) => F.get flatMap {
      case ReadState(current, handles) =>
        F.put { ReadState(
          current + 1,
          handles + ((current, (new AtomicBoolean(false), offset, limit))))
        } >> ReadFile.ReadHandle(file, current).right.point[F]
    }

    case ReadFile.Read(h @ ReadFile.ReadHandle(file, id)) => F.gets { state =>

      val check: FileSystemError \/ (AtomicBoolean, Natural, Option[Positive]) =
        toRight(state.handles.get(id))(
          FileSystemError.unknownReadHandle(h))

      check flatMap {
        case (bool, offset, limit) =>
          val jpath: JPath = toJPath(file)

          if (bool.getAndSet(true)) { // we've already read from this handle
            Vector[Data]().right
          } else {
            if (jpath.toFile.exists) {
              tryCatch[Vector[String], Vector[Data]](
                Files.readAllLines(jpath, StandardCharsets.UTF_8).asScala.toVector)(
                s"Reading failed for $file.",
                _.drop(offset.value.toInt)
                  .take(limit.map(_.value.toInt).getOrElse(Int.MaxValue)) // FIXME horrible performance
                  .traverse(str =>
                    \/.fromEither(Data.jsonParser.parseFromString(str).toEither))
                  .leftMap(err =>
                    FileSystemError.readFailed(err.toString, s"Read for $file failed.")))
            } else {
              Vector[Data]().right
            }
          }
      }
    }

    case ReadFile.Close(ReadFile.ReadHandle(_, id)) => F.modify {
      case ReadState(current, handles) =>
        ReadState(current, handles - id)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def writeFile[F[_]: Monad](implicit F: MonadState_[F, WriteState]) = λ[WriteFile ~> F] {
    case WriteFile.Open(file) => F.get flatMap {
      case WriteState(current, handles) =>
        F.put { WriteState(
          current + 1,
          handles + current)
        } >> WriteFile.WriteHandle(file, current).right.point[F]
    }

    case WriteFile.Write(h @ WriteFile.WriteHandle(path, id), data) => F.gets { state =>
      implicit val codec: DataCodec = DataCodec.Precise

      val check: Option[FileSystemError] =
        state.handles.contains(id).fold(
          None,
          FileSystemError.unknownWriteHandle(h).some)

      check match {
        case Some(err) => Vector(err)
        case None =>
          data.traverse(DataCodec.render).map { strs =>
            \/.fromTryCatchNonFatal[JPath] {
              Files.createDirectories(toJPath(path).getParent)
              Files.write(toJPath(path),
                strs.asJava,
                StandardCharsets.UTF_8,
                StandardOpenOption.APPEND,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)
            }
          } match {
            case Some(\/-(_)) =>
              Vector[FileSystemError]()
            case Some(-\/(err)) =>
              Vector(FileSystemError.unexpectedError(
                err.some,
                s"Error during writing to path $path."))
            case None =>
              Vector(FileSystemError.unexpectedError(
                None,
                s"Data failed to render while writing to path $path."))
          }
      }
    }

    case WriteFile.Close(WriteFile.WriteHandle(_, id)) => F.modify {
      case WriteState(current, handles) =>
        WriteState(current, handles - id)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def manageFile = λ[ManageFile ~> Task] {
    case ManageFile.Move(pair, MoveSemantics.FailIfExists) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetExists(src, dst) match {
          case Some(err) =>err.left
          case None =>
            tryCatch[Unit, Unit]({
              val contents: List[JFile] = Files.list(jsrc)
                .collect(Collectors.toList()).asScala.toList
                .map(_.toFile)
                .filter(_.isFile)

              Files.createDirectories(jdst)
              contents.foreach(f =>
                Files.move(f.toPath, jdst.resolve(f.toPath.getFileName)))
            })(s"Error moving $src to $dst with semantic FailIfExists.", κ(().right))
        }
      } flatMap {
        case err @ -\/(_) => err.point[Task]
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetExists(src, dst) match {
          case Some(err) => err.left
          case None =>
            tryCatch[JPath, Unit]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst)
            })(s"Error moving $src to $dst with semantic FailIfExists.", κ(().right))
        }
      }
    }

    case ManageFile.Move(pair, MoveSemantics.FailIfMissing) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetMissing(src, dst) match {
          case Some(err) => err.left
          case None =>
            tryCatch[Unit, Unit]({
              val contents: List[JFile] = Files.list(jsrc)
                .collect(Collectors.toList()).asScala.toList
                .map(_.toFile)
                .filter(_.isFile)
              Files.createDirectories(jdst.getParent)
              contents.foreach(f => Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            })(s"Error moving $src to $dst with semantic FailIfMissing.", κ(().right))
        }
      } flatMap {
        case err @ -\/(_) => err.point[Task]
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfTargetMissing(src, dst) match {
          case Some(err) => err.left
          case None =>
            tryCatch[JPath, Unit]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
            })(s"Error moving $src to $dst with semantic FailIfMissing.", κ(().right)),
        }
      }
    }

    case ManageFile.Move(pair, MoveSemantics.Overwrite) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left
          case None =>
            tryCatch[Unit, Unit]({
              val contents: List[JFile] = Files.list(jsrc)
                .collect(Collectors.toList()).asScala.toList
                .map(_.toFile)
                .filter(_.isFile)
              Files.createDirectories(jdst.getParent)
              contents.foreach(f => Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            })(s"Error moving $src to $dst with semantic Overwrite.", κ(().right)),
        }
      } flatMap {
        case err @ -\/(_) => err.point[Task]
        case \/-(_) => delete(src)
      }

      case ManageFile.PathPair.FileToFile(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left
          case None =>
            tryCatch[JPath, Unit]({
              Files.createDirectories(jdst.getParent)
              Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
            })(s"Error moving $src to $dst with semantic Overwrite.", κ(().right)),
        }
      }
    }

    case ManageFile.Copy(pair) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) => Task delay {
        val jsrc: JPath = toJPath(src)
        val jdst: JPath = toJPath(dst)

        failIfSourceMissing(src) match {
          case Some(err) => err.left
          case None =>
            tryCatch[Unit, Unit]({
              val contents: List[JFile] = Files.list(jsrc)
                .collect(Collectors.toList()).asScala.toList
                .map(_.toFile)
                .filter(_.isFile)

              Files.createDirectories(jdst.getParent)
              contents.foreach(f => Files.copy(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
            })(s"Error copying directory $src to directory $dst.", κ(().right)),
        }
      }

      case ManageFile.PathPair.FileToFile(src, dst) => Task delay {
        failIfSourceMissing(src) match {
          case Some(err) => err.left
          case None =>
            tryCatch[JPath, Unit](
              Files.copy(toJPath(src), toJPath(dst), StandardCopyOption.REPLACE_EXISTING))(
              s"Error copying file $src to file $dst.",
              κ(().right))
        }
      }
    }

    case ManageFile.Delete(path) => delete(path)

    case ManageFile.TempFile(path, tempFilePrefix) =>
      val tmp: Task[Throwable \/ JFile] = Task delay {
        val jpath: JPath = toJPath(path)
        val prefix: String = tempFilePrefix.fold("")( _.s)

        \/.fromTryCatchNonFatal[JFile] {
          refineType(path).fold(
            _ => { // dir
              Files.createDirectories(jpath)
              Files.createTempFile(jpath, prefix, "").toFile
            },
            _ => { // file
              Files.createDirectories(jpath.getParent)
              Files.createTempFile(jpath.getParent, prefix, "").toFile
            })
        }
      }

      EitherT.eitherT(tmp)
        .leftMap(err =>
          FileSystemError.unexpectedError(
            err.some,
            s"Error creating a temp file near $path."))
        .flatMap(file =>
          AFile.fromFile(file).toRight(
            FileSystemError.unexpectedError(
              None,
              s"Error constructing an AFile from the created temp file $file.")))
        .run
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def queryFile = λ[QueryFile ~> Task] {
    case QueryFile.ExecutePlan(lp, _) => ??? // not supported
    case QueryFile.EvaluatePlan(lp) => ??? // not supported
    case QueryFile.More(h) => ??? // not supported
    case QueryFile.Close(_) => ??? // not supported
    case QueryFile.Explain(lp) => ??? // not supported

    case QueryFile.ListContents(dir) => Task delay {
      failIfSourceMissing(dir) match {
        case Some(err) => err.left
        case None =>
          tryCatch[Set[Node], Set[Node]]({
            val contents: List[JFile] = Files.list(toJPath(dir))
              .collect(Collectors.toList()).asScala.toList
              .map(_.toFile)

            val files: List[PathSegment] = contents.collect {
              case f if f.isDirectory => DirName(f.getName).left
              case f if f.isFile => FileName(f.getName).right
            }

            files.map(quasar.fs.Node.fromSegment).toSet
          })(s"Error listing contents of $dir.", _.right)
      }
    }

    case QueryFile.FileExists(file) => Task delay {
      Files.exists(toJPath(file))
    }
  }

  ////////

  private def toJPath(path: APath): JPath = Paths.get(prettyPrint(path))

  private def delete(path: APath): Task[FileSystemError \/ Unit] = Task delay {
    val jpath: JPath = toJPath(path)

    failIfSourceMissing(path) match {
      case Some(err) => err.left
      case None => refineType(path) match {
        case -\/(_) => // directory
          val visitor = new SimpleFileVisitor[JPath] {
            override def visitFile(file: JPath, attrs: BasicFileAttributes): FileVisitResult = {
              Files.delete(file)
              FileVisitResult.CONTINUE
            }

            override def postVisitDirectory(dir: JPath, exc: IOException): FileVisitResult = {
              Files.delete(dir)
              FileVisitResult.CONTINUE
            }
          }

          tryCatch[JPath, Unit](Files.walkFileTree(jpath, visitor))(
            s"Error deleting directory $path.",
            κ(().right))

        case \/-(_) => // file
          tryCatch[Boolean, Unit](Files.deleteIfExists(jpath))(
            s"Error deleting file $path.",
            bool =>
              if (bool) ().right
              else FileSystemError.pathErr(PathError.pathNotFound(path)).left)
      }
    }
  }

  private def tryCatch[A, B](effect: A)(errMsg: String, cont: A => FileSystemError \/ B)
      : FileSystemError \/ B =
    \/.fromTryCatchNonFatal[A](effect).fold(
      err => FileSystemError.unexpectedError(err.some, errMsg).left,
      cont)

  private def failIfSourceMissing(src: APath): Option[FileSystemError] =
    if (!Files.exists(toJPath(src)))
      FileSystemError.pathErr(PathError.pathNotFound(src)).some
    else
      None

  private def failIfTargetExists(src: APath, dst: APath): Option[FileSystemError] =
    failIfSourceMissing(src) match {
      case err @ Some(_) => err
      case None =>
        if (Files.exists(toJPath(dst)))
          FileSystemError.pathErr(PathError.pathExists(dst)).some
        else
          None
    }

  private def failIfTargetMissing(src: APath, dst: APath): Option[FileSystemError] =
    failIfSourceMissing(src) match {
      case err @ Some(_) => err
      case None =>
        if (!Files.exists(toJPath(dst)))
          FileSystemError.pathErr(PathError.pathNotFound(dst)).some
        else
          None
    }

  private def readToTask: Task[State[ReadState, ?] ~> Task] =
    TaskRef(ReadState(0, Map())) map { ref =>
      new (State[ReadState, ?] ~> Task) {
        def apply[A](state: State[ReadState, A]) =
          ref.modifyS(state.run)
      }
    }

  private def writeToTask: Task[State[WriteState, ?] ~> Task] =
    TaskRef(WriteState(0, Set())) map { ref =>
      new (State[WriteState, ?] ~> Task) {
        def apply[A](state: State[WriteState, A]) =
          ref.modifyS(state.run)
      }
    }

  ////////

  def runFs: Task[FileSystem ~> Task] = for {
    read <- readToTask
    write <- writeToTask
  } yield { interpretFileSystem(
    queryFile,
    read compose readFile[State[ReadState, ?]],
    write compose writeFile[State[WriteState, ?]],
    manageFile)
  }
}

object Local {
  def apply(baseDir: JFile): Local = new Local(baseDir)
}
