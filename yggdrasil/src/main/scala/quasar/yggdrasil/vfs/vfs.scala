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

package quasar.yggdrasil.vfs

import quasar.contrib.pathy.{ADir, AFile, APath, RPath}
import quasar.contrib.scalaz.stateT, stateT._

import argonaut.{Argonaut, Parse}

import fs2.Stream
import fs2.interop.scalaz.StreamScalazOps

import pathy.Path

import scalaz.{:<:, Free, Monad, StateT}
import scalaz.concurrent.Task
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.string._
import scalaz.std.vector._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.monoid._

import scodec.bits.ByteVector

import java.util.UUID

object FreeVFS {
  import Argonaut._

  import RPath._

  private val MetaDir = Path.dir("META")
  private val PathsFile = Path.file("paths.json")
  private val IndexFile = Path.file("index.json")

  private type ST[F[_], A] = StateT[F, VFS, A]

  def init[S[_]](baseDir: ADir)(implicit IP: POSIXOp :<: S, IT: Task :<: S): Free[S, VFS] = {
    for {
      exists <- POSIX.exists[S](baseDir </> MetaDir)

      triple <- if (!exists) {
        for {
          _ <- POSIX.mkDir[S](baseDir </> MetaDir)
          metaLog <- VersionLog.init[S](baseDir </> MetaDir)

          paths = Map[AFile, Blob]()
          index = Map[ADir, Vector[RPath]]()
          _ <- persistMeta[S](paths, index).eval(metaLog)
        } yield (metaLog, paths, index)
      } else {
        for {
          metaLog <- VersionLog.init[S](baseDir </> MetaDir)

          pathsST = for {
            dir <- VersionLog.underlyingHeadDir[Free[S, ?]]

            pair <- dir match {
              case Some(dir) =>
                readMeta(dir)

              case None =>
                val paths = Map[AFile, Blob]()
                val index = Map[ADir, Vector[RPath]]()
                persistMeta[S](paths, index).map(_ => (paths, index))
            }
          } yield pair

          pair <- pathsST.eval(metaLog)
          (paths, index) = pair
        } yield (metaLog, paths, index)
      }

      (metaLog, paths, index) = triple

      blobPaths <- POSIX.ls[S](baseDir)

      dirs = blobPaths flatMap { path =>
        val back = Path.maybeDir(path).flatMap(Path.dirName).map(_.value)

        back.toList
      }

      blobs = dirs.filterNot(n => Path.dir(n) === MetaDir) flatMap { name =>
        try {
          List(Blob(UUID.fromString(name)))
        } catch {
          case _: IllegalArgumentException => Nil
        }
      }
    } yield VFS(baseDir, metaLog, paths, index, Map(), blobs.toSet)
  }

  private def persistMeta[S[_]](paths: Map[AFile, Blob], index: Map[ADir, Vector[RPath]])(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      v <- VersionLog.fresh[S]
      target <- VersionLog.underlyingDir[Free[S, ?]](v)

      pathsSink <- POSIX.openW[S](target </> PathsFile).liftM[StateT[?[_], VersionLog, ?]]

      pathsFromStrings = paths map {
        case (k, v) => Path.posixCodec.printPath(k) -> v
      }

      pathsJson = pathsFromStrings.asJson.nospaces
      pathsWriter = Stream.emit(ByteVector(pathsJson.getBytes)).to(pathsSink).run
      _ <- POSIXWithTask.generalize(pathsWriter).liftM[StateT[?[_], VersionLog, ?]]

      indexSink <- POSIX.openW[S](target </> IndexFile).liftM[StateT[?[_], VersionLog, ?]]

      indexFromStrings = index map {
        case (k, v) => Path.posixCodec.printPath(k) -> v
      }

      indexJson = indexFromStrings.asJson.nospaces
      indexWriter = Stream.emit(ByteVector(indexJson.getBytes)).to(indexSink).run
      _ <- POSIXWithTask.generalize[S](indexWriter).liftM[StateT[?[_], VersionLog, ?]]

      _ <- VersionLog.commit[S](v)
    } yield ()
  }

  private def readMeta[S[_]](dir: ADir)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VersionLog, (Map[AFile, Blob], Map[ADir, Vector[RPath]])] = {
    for {
      maybeDir <- VersionLog.underlyingHeadDir[Free[S, ?]]

      back <- maybeDir match {
        case Some(dir) =>
          for {
            pathsStream <- POSIX.openR[S](dir </> PathsFile).liftM[StateT[?[_], VersionLog, ?]]
            pathsString = pathsStream.map(_.toArray).map(new String(_)).foldMonoid
            pathsJson <- POSIXWithTask.generalize[S](pathsString.runLast).liftM[StateT[?[_], VersionLog, ?]]

            pathsFromStrings =
              pathsJson.flatMap(Parse.decodeOption[Map[String, Blob]]).getOrElse(Map())

            paths = pathsFromStrings flatMap {
              case (k, v) =>
                val maybe = for {
                  abs <- Path.posixCodec.parseAbsFile(k)
                  sandboxed <- Path.sandbox(Path.currentDir, abs)
                } yield Path.rootDir </> sandboxed

                maybe.fold(Map[AFile, Blob]())(k => Map(k -> v))
            }

            indexStream <- POSIX.openR[S](dir </> IndexFile).liftM[StateT[?[_], VersionLog, ?]]
            indexString = indexStream.map(_.toArray).map(new String(_)).foldMonoid
            indexJson <- POSIXWithTask.generalize[S](indexString.runLast).liftM[StateT[?[_], VersionLog, ?]]

            indexFromStrings =
              indexJson.flatMap(Parse.decodeOption[Map[String, Vector[RPath]]](_)).getOrElse(Map())

            index = indexFromStrings flatMap {
              case (k, v) =>
                val maybe = for {
                  abs <- Path.posixCodec.parseAbsDir(k)
                  sandboxed <- Path.sandbox(Path.currentDir, abs)
                } yield Path.rootDir </> sandboxed

                maybe.fold(Map[ADir, Vector[RPath]]())(k => Map(k -> v))
            }
          } yield (paths, index)

        case None =>
          (Map[AFile, Blob](), Map[ADir, Vector[RPath]]()).point[StateT[Free[S, ?], VersionLog, ?]]
      }
    } yield back
  }

  def scratch[S[_]](implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Blob] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]
      uuid <- POSIX.genUUID[S].liftM[ST]

      target = vfs.baseDir </> Path.dir(uuid.toString)
      blob = Blob(uuid)

      back <- if (vfs.blobs.contains(blob)) {
        for {
          _ <- POSIX.mkDir[S](target).liftM[ST]
          vlog <- VersionLog.init[S](target).liftM[ST]
          vfs2 = vfs.copy(blobs = vfs.blobs + blob, versions = vfs.versions + (blob -> vlog))
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield blob
      } else {
        scratch[S]
      }
    } yield back
  }

  def exists[F[_]: Monad](path: AFile): StateT[F, VFS, Boolean] =
    StateTContrib.get[F, VFS].map(_.paths.contains(path))

  def ls[F[_]: Monad](parent: ADir): StateT[F, VFS, List[RPath]] = {
    StateTContrib.get[F, VFS] map { vfs =>
      vfs.index.get(parent).map(_.toList).getOrElse(Nil)
    }
  }

  def link[S[_]](from: Blob, to: AFile)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(to)) {
        false.point[ST[Free[S, ?], ?]]
      } else {
        val paths2 = vfs.paths + (to -> from)
        val index2 = vfs.index |+| computeSubIndex(to)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      }
    } yield success
  }

  def move[S[_]](from: AFile, to: AFile)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(from) && !vfs.paths.contains(to)) {
        // we could delegate to delete >> link, but that would persist metadata twice
        val blob = vfs.paths(from)
        val paths2 = vfs.paths - from + (to -> blob)
        val index2 = computeIndex(paths2.keys.toList)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      } else {
        false.point[ST[Free[S, ?], ?]]
      }
    } yield success
  }

  def delete[S[_]](target: AFile)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Boolean] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      success <- if (vfs.paths.contains(target)) {
        val paths2 = vfs.paths - target
        val index2 = computeIndex(paths2.keys.toList)

        for {
          metaLog2 <- persistMeta[S](paths2, index2).exec(vfs.metaLog).liftM[ST]
          vfs2 = vfs.copy(metaLog = metaLog2, paths = paths2, index = index2)
          _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
        } yield true
      } else {
        false.point[ST[Free[S, ?], ?]]
      }
    } yield success
  }

  def readPath[F[_]: Monad](path: AFile): StateT[F, VFS, Option[Blob]] =
    StateTContrib.get[F, VFS].map(_.paths.get(path))

  def underlyingDir[S[_]](blob: Blob, version: Version)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, ADir] =
    withVLog(blob)(VersionLog.underlyingDir[Free[S, ?]](version))

  private def blobVLog[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, VersionLog] = {
    for {
      vfs <- StateTContrib.get[Free[S, ?], VFS]

      vlog <- vfs.versions.get(blob) match {
        case Some(vlog) => vlog.point[ST[Free[S, ?], ?]]
        case None =>
          for {
            vlog <- VersionLog.init[S](vfs.baseDir </> Path.dir(blob.value.toString)).liftM[ST]
            vfs2 = vfs.copy(versions = vfs.versions + (blob -> vlog))
            _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
          } yield vlog
      }
    } yield vlog
  }

  private def withVLog[S[_], A](blob: Blob)(st: StateT[Free[S, ?], VersionLog, A])(
    implicit
      IP: POSIXOp :<: S,
      IT: Task :<: S): StateT[Free[S, ?], VFS, A] = {

    for {
      vlog <- blobVLog[S](blob)
      pair <- st(vlog).liftM[ST]
      (vlog2, a) = pair

      vfs <- StateTContrib.get[Free[S, ?], VFS]
      vfs2 = vfs.copy(versions = vfs.versions.updated(blob, vlog2))
      _ <- StateTContrib.put[Free[S, ?], VFS](vfs2)
    } yield a
  }

  def headOfBlob[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Option[Version]] =
    blobVLog[S](blob).map(_.head)

  def fresh[S[_]](blob: Blob)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Version] =
    withVLog(blob)(VersionLog.fresh[S])

  def commit[S[_]](blob: Blob, version: Version)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VFS, Unit] =
    withVLog(blob)(VersionLog.commit[S](version))

  private def computeIndex(paths: List[APath]): Map[ADir, Vector[RPath]] =
    paths.foldMap(computeSubIndex(_))

  // computes the subset of the index pertaining to the given path, recursively
  private def computeSubIndex(path: APath): Map[ADir, Vector[RPath]] = {
    Path.peel(path) match {
      case Some((dir, dirOrFile)) =>
        val rpath = dirOrFile.fold(Path.dir1, Path.file1)
        computeSubIndex(dir) + (dir -> Vector(rpath))

      case None => Map()
    }
  }
}

final case class VFS(
    baseDir: ADir,
    metaLog: VersionLog,
    paths: Map[AFile, Blob],
    index: Map[ADir, Vector[RPath]],
    versions: Map[Blob, VersionLog],    // always getOrElse this with VersionLog.init
    blobs: Set[Blob])

object VFS extends ((ADir, VersionLog, Map[AFile, Blob], Map[ADir, Vector[RPath]], Map[Blob, VersionLog], Set[Blob]) => VFS) {

}
