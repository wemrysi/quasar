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
import quasar.common.PhaseResult
import quasar.contrib.pathy.{prettyPrint, AFile, APath, PathSegment}
import quasar.fp.TaskRef
import quasar.fp.numeric.{Natural, Positive}
import quasar.fp.ski.κ
import quasar.fs.ReadFile.ReadHandle
import quasar.fs.WriteFile.WriteHandle

import java.io.{File => JFile}
import java.nio.charset.StandardCharsets
import java.nio.file.{
  Files,
  Path => JPath,
  Paths,
  StandardCopyOption,
  StandardOpenOption
}
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.stream.Collectors

import scala.collection.JavaConverters._

import pathy.Path.{file1, refineType, DirName, FileName}
import scalaz.{~>, \/, -\/, \/-, EitherT}
import scalaz.Scalaz._
import scalaz.concurrent.Task

/** A local file system implementation.
  *
  * The implementations are not guaranteed to be atomic.
  */
object Local {

  final case class ReadState(
    current: Long,
    handles: Map[Long, (AtomicBoolean, Natural, Option[Positive])])

  final case class WriteState(
    current: Long,
    handles: Set[Long])

  type Result[A] = EitherT[Task, LocalFileSystemError, A]

  final case class StatefulResult[S, A](updateAndCompute: S => (Task[S], Result[A]))

  type WriteResult[A] = Task[StatefulResult[WriteState, A]]
  type ReadResult[A] = Task[StatefulResult[ReadState, A]]

  type FSError[A] = FileSystemError \/ A
  type LocalFSError[A] = LocalFileSystemError \/ A

  def readFile = λ[ReadFile ~> ReadResult] {
    case ReadFile.Open(file, offset, limit) => Task delay {
      val res: ReadState => (Task[ReadState], Result[FileSystemError \/ ReadHandle]) = {
        case ReadState(current, handles) =>
	  val update: Task[ReadState] = Task delay {
            ReadState(
              current + 1,
              handles + ((current, (new AtomicBoolean(false), offset, limit))))
	  }

          val compute: Result[FileSystemError \/ ReadHandle] = EitherT.rightT {
            Task.delay(ReadHandle(file, current).right[FileSystemError])
          }

	  (update, compute)
      }

      StatefulResult[ReadState, FileSystemError \/ ReadHandle](res)
    }

    case ReadFile.Read(h @ ReadHandle(file, id)) => Task delay {
      val res: ReadState => (Task[ReadState], Result[FileSystemError \/ Vector[Data]]) = {
        case state @ ReadState(_, handles) =>
	  val update: Task[ReadState] = Task.now(state)

          val compute: Result[FileSystemError \/ Vector[Data]] = EitherT.eitherT {
            Task.delay {
              val check: FSError[(AtomicBoolean, Natural, Option[Positive])] =
                toRight(handles.get(id))(
                  FileSystemError.unknownReadHandle(h))

              check match {
                case -\/(err) =>
                  err.left.right

                case \/-((bool, offset, limit)) =>
                  val jpath: JPath = toJPath(file)

                  if (bool.getAndSet(true)) { // we've already read from this handle
                    Vector[Data]().right.right
                  } else {
                    if (jpath.toFile.exists) {
                      \/.fromTryCatchNonFatal[Vector[String]](
                        Files.readAllLines(jpath, StandardCharsets.UTF_8).asScala.toVector).fold(
                        err => LocalFileSystemError.readFailed(jpath, err).left,
                        _.drop(offset.value.toInt)
                          .take(limit.map(_.value.toInt).getOrElse(Int.MaxValue)) // FIXME horrible performance
                          .traverse(str =>
                            \/.fromEither(Data.jsonParser.parseFromString(str).toEither))
                          .leftMap(err =>
                            FileSystemError.readFailed(err.toString, s"Read for $file failed"))
                          .right)
                    } else {
                      // FIXME apparently read on a non-existent file is equivalent to reading the empty file??!!
                      Vector[Data]().right.right
                    }
                  }
              }
            }
          }

	  (update, compute)
        }

      StatefulResult[ReadState, FileSystemError \/ Vector[Data]](res)
    }

    case ReadFile.Close(ReadHandle(_, id)) => Task delay {
      val res: ReadState => (Task[ReadState], Result[Unit]) = {
        case ReadState(current, handles) =>
          val update = Task delay {
	    ReadState(current, handles - id)
	  }

	  val compute = EitherT.rightT[Task, LocalFileSystemError, Unit](Task.now(()))

	  (update, compute)
      }

      StatefulResult[ReadState, Unit](res)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def writeFile = λ[WriteFile ~> WriteResult] {
    case WriteFile.Open(file) => Task delay {
      val res: WriteState => (Task[WriteState], Result[FileSystemError \/ WriteHandle]) = {
        case WriteState(current, handles) =>
	  val update = Task delay {
            WriteState(current + 1, handles + current)
          }

          val compute: Result[FileSystemError \/ WriteHandle] =
            EitherT.rightT {
              Task.delay(WriteHandle(file, current).right[FileSystemError])
            }

	  (update, compute)
      }

      StatefulResult[WriteState, FileSystemError \/ WriteHandle](res)
    }

    case WriteFile.Write(h @ WriteHandle(path, id), data) => Task delay {
      val res: WriteState => (Task[WriteState], Result[Vector[FileSystemError]]) = {
        case state @ WriteState(_, handles) =>

	  val update = Task.now(state)

          val compute = EitherT.eitherT(Task.delay {
	    if (!handles.contains(id)) {
              Vector(FileSystemError.unknownWriteHandle(h)).right
            } else {
              implicit val codec: DataCodec = DataCodec.Precise

              val (errs, strs): (Vector[Data], Vector[String]) = data.foldMap { d =>
                DataCodec.render(d).fold(
                  (Vector(d), Vector[String]()))(
                  (s: String) => (Vector[Data](), Vector(s)))
              }

              val jpath: JPath = toJPath(path)

              \/.fromTryCatchNonFatal[JPath] {
                Files.createDirectories(jpath.getParent)
                Files.write(jpath,
                  strs.asJava,
                  StandardCharsets.UTF_8,
                  StandardOpenOption.APPEND,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.WRITE)
              }.fold(
                err => LocalFileSystemError.writeFailed(toJPath(path), err).left,
                _ => {
                  val partial = if (errs.isEmpty)
                    Vector()
                  else
                    Vector(FileSystemError.partialWrite(errs.size))

                  (errs.map(FileSystemError.writeFailed(_, s"Write failed for path $path")) ++ partial).right
                })
            }
	  })

	  (update, compute)
        }

      StatefulResult[WriteState, Vector[FileSystemError]](res)
    }

    case WriteFile.Close(WriteHandle(_, id)) => Task delay {
      val res: WriteState => (Task[WriteState], Result[Unit]) = {
        case WriteState(current, handles) =>
          val update = Task delay {
	    WriteState(current, handles - id)
	  }

	  val compute = EitherT.rightT[Task, LocalFileSystemError, Unit](Task.now(()))

	  (update, compute)
      }

      StatefulResult[WriteState, Unit](res)
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def manageFile = λ[ManageFile ~> Result] {
    case ManageFile.Move(pair, sem @ MoveSemantics.FailIfExists) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfTargetExists(src, dst) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[Unit]({
                  Files.createDirectories(jdst)

                  listFiles(jsrc).foreach(f =>
                    Files.move(f.toPath, jdst.resolve(f.toPath.getFileName)))
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        } flatMapF {
          case err @ -\/(_) => Task.point(err.right)
          case \/-(_) => delete(src)
        }

      case ManageFile.PathPair.FileToFile(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfTargetExists(src, dst) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[JPath]({
                  Files.createDirectories(jdst.getParent)
                  Files.move(jsrc, jdst)
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        }
    }

    case ManageFile.Move(pair, sem @ MoveSemantics.FailIfMissing) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfTargetMissing(src, dst) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[Unit]({
                  Files.createDirectories(jdst.getParent)

                  listFiles(jsrc).foreach(f =>
                    Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        } flatMapF {
          case err @ -\/(_) => Task.point(err.right)
          case \/-(_) => delete(src)
        }

      case ManageFile.PathPair.FileToFile(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfTargetMissing(src, dst) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[JPath]({
                  Files.createDirectories(jdst.getParent)
                  Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        }
    }

    case ManageFile.Move(pair, sem @ MoveSemantics.Overwrite) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfSourceMissing(src) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[Unit]({
                  Files.createDirectories(jdst.getParent)

                  listFiles(jsrc).foreach(f =>
                    Files.move(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        } flatMapF {
          case err @ -\/(_) => Task.point(err.right)
          case \/-(_) => delete(src)
        }

      case ManageFile.PathPair.FileToFile(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfSourceMissing(src) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[JPath]({
                  Files.createDirectories(jdst.getParent)
                  Files.move(jsrc, jdst, StandardCopyOption.REPLACE_EXISTING)
                }).fold(
                  err => LocalFileSystemError.MoveFailed(pair, sem, err).left,
                  κ(().right.right))
            }
          }
        }
    }

    case ManageFile.Copy(pair) => pair match {
      case ManageFile.PathPair.DirToDir(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            val jsrc: JPath = toJPath(src)
            val jdst: JPath = toJPath(dst)

            failIfSourceMissing(src) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[Unit]({
                  Files.createDirectories(jdst.getParent)

                  listFiles(jsrc).foreach(f =>
                    Files.copy(f.toPath, jdst, StandardCopyOption.REPLACE_EXISTING))
                }).fold(
                  err => LocalFileSystemError.CopyFailed(pair, err).left,
                  κ(().right.right))
            }
          }
        }

      case ManageFile.PathPair.FileToFile(src, dst) =>
        EitherT.eitherT[Task, LocalFileSystemError, FSError[Unit]] {
          Task.delay {
            failIfSourceMissing(src) match {
              case Some(err) => err.left.right
              case None =>
                \/.fromTryCatchNonFatal[JPath](
                  Files.copy(toJPath(src), toJPath(dst), StandardCopyOption.REPLACE_EXISTING)).fold(
                  err => LocalFileSystemError.CopyFailed(pair, err).left,
                  κ(().right.right))
            }
          }
        }
    }

    case ManageFile.Delete(path) => EitherT.eitherT(delete(path))

    case ManageFile.TempFile(path, prefix) =>
      EitherT.eitherT[Task, LocalFileSystemError, FSError[AFile]] {
        Task.delay {
          val uuid: String = UUID.randomUUID().toString
          val fileName: FileName = FileName(prefix.fold("")(_.s) + uuid)
          val jpath: JPath = toJPath(path)

          \/.fromTryCatchNonFatal[JPath] {
            refineType(path).fold(
              _ => { // directory
                Files.createDirectories(jpath)
                Files.createFile(Paths.get(jpath.toFile.getPath, fileName.value))
              },
              _ => { // file
                Files.createDirectories(jpath.getParent)
                Files.createFile(Paths.get(jpath.toFile.getParent, fileName.value))
              })
            }.fold(
              err => LocalFileSystemError.tempFileCreationFailed(toJPath(path), err).left,
              κ((nearDir(path) </> file1(fileName)).right.right))
        }
      }
  }

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def queryFile = λ[QueryFile ~> Result] {
    case QueryFile.ExecutePlan(_, _) => EitherT.rightT(Task.delay {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.ExecutePlan not supported by the local file system.").left)
    })

    case QueryFile.EvaluatePlan(_) => EitherT.rightT(Task.delay {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.EvaluatePlan not supported by the local file system.").left)
    })

    case QueryFile.More(h) => EitherT.rightT(Task.delay {
      FileSystemError.unsupportedOperation(
        "QueryFile.More not supported by the local file system.").left
    })

    case QueryFile.Close(_) => EitherT.rightT(Task.point(()))

    case QueryFile.Explain(lp) => EitherT.rightT(Task.delay {
      (Vector[PhaseResult](), FileSystemError.unsupportedOperation(
        "QueryFile.Explain not supported by the local file system.").left)
    })

    case QueryFile.ListContents(dir) =>
      EitherT.eitherT[Task, LocalFileSystemError, FSError[Set[Node]]] {
        Task.delay {
          failIfSourceMissing(dir) match {
            case Some(err) => err.left.right
            case None =>
              \/.fromTryCatchNonFatal[FSError[Set[Node]]] {
                val files: List[PathSegment] =
                  listFilesAndDirs(toJPath(dir)).collect {
                    case f if f.isDirectory => DirName(f.getName).left
                    case f if f.isFile => FileName(f.getName).right
                  }

                files.map(Node.fromSegment).toSet.right
              }.leftMap(e => LocalFileSystemError.listContentsFailed(toJPath(dir), e))
          }
        }
      }

    case QueryFile.FileExists(file) => EitherT.rightT {
      Task.delay(Files.exists(toJPath(file)))
    }
  }

  ////////

  private def toJPath(path: APath): JPath = Paths.get(prettyPrint(path))

  private def listFilesAndDirs(src: JPath): List[JFile] =
    Files.list(src)
      .collect(Collectors.toList()).asScala.toList
      .map(_.toFile)

  private def listFiles(src: JPath): List[JFile] =
    listFilesAndDirs(src).filter(_.isFile)

  private def delete(path: APath): Task[LocalFSError[FSError[Unit]]] =
    Task delay {
      val jpath: JPath = toJPath(path)

      failIfSourceMissing(path) match {
        case Some(err) => err.left.right
        case None => refineType(path) match {
          case -\/(_) => // directory

            // ported from quasar.precog.util.IOUtils
            // this works but using a `java.nio.file.SimpleFileVisitor` does not
            // (recursive delete fails when file gets into an inexplicable permission state)
            @SuppressWarnings(Array("org.wartremover.warts.Null"))
            def listFiles(f: JFile): Task[Vector[JFile]] = Task delay {
              f.listFiles match {
                case null => Vector()
                case xs => xs.toVector
              }
            }

            @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
            def recursiveDeleteSeq(files: Seq[JFile]): Task[Unit] = {
              files.toList match {
                case Nil => Task.now(())
                case hd :: tl => recursiveDelete(hd).flatMap(_ => recursiveDeleteSeq(tl))
              }
            }

            @SuppressWarnings(Array("org.wartremover.warts.Recursion", "org.wartremover.warts.NonUnitStatements"))
            def recursiveDelete(file: JFile): Task[Unit] = {
              def del(): Unit = { file.delete(); () }

              if (!file.isDirectory) Task.delay(del())
              else listFiles(file) flatMap {
                case Vector() => Task.delay(del())
                case xs => recursiveDeleteSeq(xs).flatMap(_ => Task.delay(del()))
              }
            }

            \/.fromTryCatchNonFatal[Unit](recursiveDelete(jpath.toFile).unsafePerformSync).fold(
              err => LocalFileSystemError.deleteDirFailed(jpath, err).left,
              κ(().right.right))

          case \/-(_) => // file
            \/.fromTryCatchNonFatal[Boolean](Files.deleteIfExists(jpath)).fold(
              err => LocalFileSystemError.deleteFileFailed(jpath, err).left,
              bool =>
                if (bool) ().right.right
                else FileSystemError.pathErr(PathError.pathNotFound(path)).left.right)
        }
      }
    }

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

  private def resultToTask = λ[Result ~> Task] {
    _.run.flatMap {
      _.fold(err => Task.fail(err.throwable), Task.point(_))
    }
  }

  private def readToTask: Task[ReadResult ~> Task] =
    TaskRef(ReadState(0, Map())) map { ref =>
      new (ReadResult ~> Task) {
        def apply[A](result: ReadResult[A]) =
          for {
            res <- result
            back <- ref.twoPhaseModifyT[A](res.updateAndCompute(_).rightMap(resultToTask))
          } yield back
      }
   }

  private def writeToTask: Task[WriteResult ~> Task] =
    TaskRef(WriteState(0, Set())) map { ref =>
      new (WriteResult ~> Task) {
        def apply[A](result: WriteResult[A]) =
          for {
            res <- result
            back <- ref.twoPhaseModifyT[A](res.updateAndCompute(_).rightMap(resultToTask))
          } yield back
      }
    }

  ////////

  def runFs: Task[FileSystem ~> Task] = for {
    read <- readToTask
    write <- writeToTask
  } yield { interpretFileSystem(
    resultToTask compose queryFile,
    read compose readFile,
    write compose writeFile,
    resultToTask compose manageFile)
  }
}
