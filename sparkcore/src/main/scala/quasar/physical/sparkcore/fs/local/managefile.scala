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

package quasar.physical.sparkcore.fs.local

import quasar.Predef._
import quasar.fs._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fp.free._
import quasar.fs.ManageFile._
import quasar.fs.ManageFile.MoveSemantics._
import quasar.fs.ManageFile.MoveScenario._

import java.nio.file._

import org.apache.commons.io.FileUtils
import pathy.Path.posixCodec
import scalaz._
import Scalaz._
import scalaz.concurrent.Task

object managefile {

  private def toNioPath(path: APath): Path =
    Paths.get(posixCodec.unsafePrintPath(path))

  private def toAFile(path: Path): Option[AFile] = {
    val maybeUnboxed = posixCodec.parseAbsFile(path.toString)
    maybeUnboxed.map(sandboxAbs(_))
  }

  def interpret[S[_]](implicit
    s0: Task :<: S
  ): ManageFile ~> Free[S, ?] =
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) => moveIt(sf, df, semantics)(moveFile _)
        case Move(DirToDir(sd, dd), semantics) => moveIt(sd, dd,semantics)(moveDir _)
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  def moveFile(src: APath, dst: APath):FileSystemError \/ Unit =
    \/.fromTryCatchNonFatal(
      Files.move(toNioPath(src), toNioPath(dst), StandardCopyOption.REPLACE_EXISTING)
    ) .leftMap {
      case e => pathErr(invalidPath(dst, e.getMessage()))
    }.void

  def moveDir(src: APath, dst: APath) =
    \/.fromTryCatchNonFatal{
      val deleted = FileUtils.deleteDirectory(toNioPath(dst).toFile())
      FileUtils.moveDirectory(toNioPath(src).toFile(), toNioPath(dst).toFile())
    }
      .leftMap {
      case e => pathErr(invalidPath(dst, e.getMessage()))
    }.void

  private def moveIt[S[_]](src: APath, dst: APath, semantics: MoveSemantics)
  (makeMove: (APath, APath) => FileSystemError \/ Unit)
    (implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ Unit] = {

    def failBecauseExists = PathErr(InvalidPath(dst,
      "Can not move to destination that already exists if semnatics == failIfExists"))
    def failBecauseMissing = PathErr(InvalidPath(dst,
      "Can not move to destination that does not exists if semnatics == failIfMissing"))
    
    injectFT[Task, S].apply{
      Task.delay {
        semantics match {
          case Overwrite => makeMove(src, dst)
          case FailIfExists => if(Files.exists(toNioPath(dst))) -\/(failBecauseExists) else makeMove(src, dst)
          case FailIfMissing => if(Files.notExists(toNioPath(dst))) -\/(failBecauseMissing) else makeMove(src, dst)
        }
      }
    }}


  private def delete[S[_]](path: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ Unit] = {
    val task: Task[FileSystemError \/ Unit] = Task.delay {
      \/.fromTryCatchNonFatal(Files.delete(toNioPath(path)))
        .leftMap {
        case e: NoSuchFileException => pathErr(invalidPath(path, "File does not exist"))
        case e: DirectoryNotEmptyException => pathErr(invalidPath(path, "Directory is not empty"))
        case e => pathErr(invalidPath(path, e.getMessage()))
      }
    }
    injectFT[Task, S].apply(task)
  }

  private def tempFile[S[_]](near: APath)(implicit
    s0: Task :<: S
  ): Free[S, FileSystemError \/ AFile] = {

    def handleCreationError: Throwable => FileSystemError = {
      case e: NoSuchFileException => pathErr(invalidPath(
        near,
        s"Could not create temp file in dir $near"))
      case e: FileAlreadyExistsException => pathErr(invalidPath(
        near,
        s"File with the same name already exists: $e.getFile"))
      case e: FileSystemException if e.getMessage.contains("Not a directory") => pathErr(invalidPath(
        near,
        s"Provided $near is not a directory"))

      case e => pathErr(invalidPath(near, e.getMessage()))
    }

    def fileToMaybeAFile: Path => FileSystemError \/ AFile = f =>
        toAFile(f) \/> (pathErr(invalidPath(near, s"Could not create temp file in dir $near")))
    
    val task: Task[FileSystemError \/ AFile] = Task.delay {
      val posix = posixCodec.unsafePrintPath(near) + "/"
      val prefix = "quasar"
      val suffix = ".tmp"
      \/.fromTryCatchNonFatal(Files.createTempFile(Paths.get(posix), prefix, suffix))
        .leftMap(handleCreationError)
        .flatMap(fileToMaybeAFile)        
    }
    injectFT[Task, S].apply(task)
  }

}
