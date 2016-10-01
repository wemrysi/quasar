/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.sparkcore.fs.hdfs

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.effect.Failure
import quasar.fp.free._
import quasar.fs._,
  FileSystemError._,
  ManageFile._, ManageFile.MoveScenario._,
  PathError._
import quasar.fs.impl.ensureMoveSemantics

import java.lang.Exception
import scala.util.control.NonFatal

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import pathy.Path._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

object managefile {

  def chrooted[S[_]](prefix: ADir, fileSystem: () => FileSystem)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret(fileSystem)) compose chroot.manageFile[ManageFile](prefix)

  def interpret[S[_]](fileSystem: () => FileSystem)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): ManageFile ~> Free[S, ?] = {

    val hdfs: FileSystem = fileSystem()

    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists(hdfs)(_), semantics).toLeft(()) *>
            move(sf, df, hdfs).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists(hdfs)(_), semantics).toLeft(()) *>
            move(sd, dd, hdfs).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path, hdfs)
        case TempFile(near) => tempFile(near)
      }
    }}

  private def toPath(apath: APath): Task[Path] = Task.delay {
    new Path(posixCodec.unsafePrintPath(apath))
  }

  def pathExists[S[_]](hdfs: FileSystem)(f: APath)(
    implicit s0: Task :<: S): Free[S, Boolean] =
    lift(toPath(f).map(path => hdfs.exists(path))).into[S]

  private def move[S[_]](src: APath, dst: APath, hdfs: FileSystem)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, Unit] = {
    val move: Task[PhysicalError \/ Unit] = (for {
      srcPath <- toPath(src)
      dstPath <- toPath(dst)
    } yield {
      val deleted = hdfs.delete(dstPath, true)
      hdfs.rename(srcPath, dstPath)
    }).as(().right[PhysicalError]).handle {
      case NonFatal(ex : Exception) => UnhandledFSError(ex).left[Unit]
    }

    Failure.Ops[PhysicalError, S].unattempt(lift(move).into[S])
  }

  private def delete[S[_]](apath: APath, hdfs: FileSystem)(implicit
    s0: Task :<: S,
    s1: PhysErr :<: S
  ): Free[S, FileSystemError \/ Unit] = {
    val delete: Task[FileSystemError \/ Unit] = toPath(apath).map { path =>
      if(!hdfs.exists(path))
        pathErr(pathNotFound(apath)).left[Unit]
      else {
        hdfs.delete(path, true).right[FileSystemError]
      }
    }.map(_.as(()))

    val deleteHandled: Task[PhysicalError \/ (FileSystemError \/ Unit)] =
      delete.map(_.right[PhysicalError]).handle {
        case NonFatal(e : Exception) => UnhandledFSError(e).left[FileSystemError \/ Unit]
      }

    Failure.Ops[PhysicalError, S].unattempt(lift(deleteHandled).into[S])
  }
  
  private def tempFile[S[_]](near: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ AFile] = injectFT[Task, S].apply{
    Task.delay {
      val parent: ADir = refineType(near).fold(d => d, fileParent(_))
      val random = scala.util.Random.nextInt().toString
        (parent </> file(s"quasar-$random.tmp")).right[FileSystemError]
    }
  }
}
