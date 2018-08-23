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

package quasar.precog.util

import org.slf4s.Logging
import scalaz.effect.IO

import java.io.File
import java.nio.file.Files

object IOUtils extends Logging {

  def overwriteFile(s: String, f: File): IO[Unit] = writeToFile(s, f, append = false)

  def writeToFile(s: String, f: File, append: Boolean): IO[Unit] = IO {
    Files.write(f.toPath, s.getBytes)
  }

  /** Performs a safe write to the file. Returns true
    * if the file was completely written, false otherwise
    */
  def safeWriteToFile(s: String, f: File): IO[Boolean] = {
    val tmpFile = new File(f.getParentFile, f.getName + "-" + System.nanoTime + ".tmp")

    overwriteFile(s, tmpFile) flatMap { _ =>
      IO(tmpFile.renameTo(f)) // TODO: This is only atomic on POSIX systems
    }
  }

  def recursiveDelete(files: Seq[File]): IO[Unit] = {
    files.toList match {
      case Nil      => IO(())
      case hd :: tl => recursiveDelete(hd).flatMap(_ => recursiveDelete(tl))
    }
  }

  def listFiles(f: File): IO[Array[File]] = IO {
    f.listFiles match {
      case null => Array()
      case xs   => xs
    }
  }

  /** Deletes `file`, recursively if it is a directory. */
  def recursiveDelete(file: File): IO[Unit] = {
    def del(): Unit = { file.delete(); () }

    if (!file.isDirectory) IO(del())
    else listFiles(file) flatMap {
      case Array() => IO(del())
      case xs      => recursiveDelete(xs).map(_ => del())
    }
  }

  /** Recursively deletes empty directories, stopping at the first
    * non-empty dir.
    */
  def recursiveDeleteEmptyDirs(startDir: File, upTo: File): IO[Unit] = {
    if (startDir == upTo) {
      IO { log.debug("Stopping recursive clean at root: " + upTo) }
    } else if (startDir.isDirectory) {
      if (Option(startDir.list).exists(_.length == 0)) {
        IO {
          startDir.delete()
        }.flatMap { _ =>
          recursiveDeleteEmptyDirs(startDir.getParentFile, upTo)
        }
      } else {
        IO { log.debug("Stopping recursive clean on non-empty directory: " + startDir) }
      }
    } else {
      IO { log.warn("Asked to clean a non-directory: " + startDir) }
    }
  }

  def createTmpDir(prefix: String): IO[File] = IO {
    Files.createTempDirectory(prefix).toFile
  }
}
