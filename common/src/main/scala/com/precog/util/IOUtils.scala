/*
 *  ____    ____    _____    ____    ___     ____
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.util

import java.io._
import java.util.Properties
import org.slf4s.Logging
import scala.collection.JavaConverters._
import scalaz.effect.IO
import scalaz.syntax.semigroup._
import java.nio.file.Files

object IOUtils extends Logging {
  final val Utf8Charset = java.nio.charset.Charset forName "UTF-8"

  val dotDirs = "." :: ".." :: Nil

  def isNormalDirectory(f: File) = f.isDirectory && !dotDirs.contains(f.getName)

  def readFileToString(f: File): IO[String] = IO {
    new String(Files.readAllBytes(f.toPath), Utf8Charset)
  }

  def readPropertiesFile(s: String): IO[Properties] = readPropertiesFile { new File(s) }

  def readPropertiesFile(f: File): IO[Properties] = IO {
    val props = new Properties
    props.load(new FileReader(f))
    props
  }

  def overwriteFile(s: String, f: File): IO[PrecogUnit] = writeToFile(s, f, append = false)
  def writeToFile(s: String, f: File, append: Boolean): IO[PrecogUnit] = IO {
    Files.write(f.toPath, s.getBytes);
    PrecogUnit
  }

  def writeSeqToFile[A](s0: Seq[A], f: File): IO[PrecogUnit] = IO {
    Files.write(f.toPath, s0.map("" + _: CharSequence).asJava, Utf8Charset)
    PrecogUnit
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

  def makeDirectory(dir: File): IO[PrecogUnit] = IO {
    if (dir.isDirectory || dir.mkdirs) PrecogUnit
    else throw new IOException("Failed to create directory " + dir)
  }

  def recursiveDelete(files: Seq[File]): IO[PrecogUnit] = {
    files.toList match {
      case Nil      => IO(PrecogUnit)
      case hd :: tl => recursiveDelete(hd) flatMap (_ => recursiveDelete(tl))
    }
  }

  def listFiles(f: File): IO[Array[File]] = IO {
    f.listFiles match {
      case null => Array()
      case xs   => xs
    }
  }

  /** Deletes `file`, recursively if it is a directory. */
  def recursiveDelete(file: File): IO[PrecogUnit] = {
    def del(): PrecogUnit = { file.delete() ; PrecogUnit }

    if (!file.isDirectory) IO(del())
    else listFiles(file) flatMap {
      case Array() => IO(del())
      case xs      => recursiveDelete(xs) map (_ => del())
    }
  }

  /** Recursively deletes empty directories, stopping at the first
    * non-empty dir.
    */
  def recursiveDeleteEmptyDirs(startDir: File, upTo: File): IO[PrecogUnit] = {
    if (startDir == upTo) {
      IO { log.debug("Stopping recursive clean at root: " + upTo); PrecogUnit }
    } else if (startDir.isDirectory) {
      if (Option(startDir.list).exists(_.length == 0)) {
        IO {
          startDir.delete()
        }.flatMap { _ =>
          recursiveDeleteEmptyDirs(startDir.getParentFile, upTo)
        }
      } else {
        IO { log.debug("Stopping recursive clean on non-empty directory: " + startDir); PrecogUnit }
      }
    } else {
      IO { log.warn("Asked to clean a non-directory: " + startDir); PrecogUnit }
    }
  }

  def createTmpDir(prefix: String): IO[File] = IO {
    Files.createTempDirectory(prefix).toFile
  }

  def copyFile(src: File, dest: File): IO[PrecogUnit] = IO {
    Files.copy(src.toPath, dest.toPath)
    PrecogUnit
  }
}
