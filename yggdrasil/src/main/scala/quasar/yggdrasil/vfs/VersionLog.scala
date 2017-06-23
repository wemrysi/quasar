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

import quasar.contrib.pathy.{ADir, RDir, RFile}

import argonaut.{Argonaut, CodecJson, Parse}

import fs2.Stream
import fs2.interop.scalaz.StreamScalazOps
import fs2.util.Catchable

import pathy.Path

import scalaz.{~>, :<:, EitherT, Free, Monad, StateT}
import scalaz.concurrent.Task
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.either._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

import scodec.bits.ByteVector

import scala.util.Either

import java.util.UUID

final case class VersionLog(baseDir: ADir, committed: List[Version], versions: Set[Version]) {
  def head: Option[Version] = committed.headOption
}

// TODO implement VERSION files
object VersionLog {
  import Argonaut._

  private type ST[F[_], A] = StateT[F, VersionLog, A]

  private val VersionsJson: RFile = Path.file("versions.json")
  private val VersionsJsonNew: RFile = Path.file("versions.json.new")

  // keep the 5 most recent versions, by default
  private val KeepLimit = 5

  // TODO failure recovery
  def init[S[_]](baseDir: ADir)(implicit IP: POSIXOp :<: S, IT: Task :<: S): Free[S, VersionLog] = {
    for {
      exists <- POSIX.exists[S](baseDir </> VersionsJson)

      committed <- if (exists) {
        for {
          fileStream <- POSIX.openR[S](baseDir </> VersionsJson)

          // TODO character encoding!
          fileString = fileStream.map(_.toArray).map(new String(_)).foldMonoid
          json <- POSIXWithTask.generalize[S](fileString.runLast)
        } yield json.flatMap(Parse.decodeOption[List[Version]](_)).getOrElse(Nil)
      } else {
        for {
          vnew <- POSIX.openW[S](baseDir </> VersionsJsonNew)

          json = List[Version]().asJson.nospaces
          // TODO character encoding!
          writer = Stream.emit(ByteVector(json.getBytes)).to(vnew).run
          _ <- POSIXWithTask.generalize(writer)

          _ <- POSIX.move[S](baseDir </> VersionsJsonNew, baseDir </> VersionsJson)
        } yield Nil
      }

      paths <- POSIX.ls[S](baseDir)

      versions = for {
        path <- paths
        dir <- Path.maybeDir(path).toList
        dirName <- Path.dirName(dir).toList

        version <- try {
          Version(UUID.fromString(dirName.value)) :: Nil
        } catch {
          case _: IllegalArgumentException => Nil
        }
      } yield version
    } yield VersionLog(baseDir, committed, versions.toSet)
  }

  def fresh[S[_]](implicit I: POSIXOp :<: S): StateT[Free[S, ?], VersionLog, Version] = {
    for {
      log <- getS[Free[S, ?], VersionLog]
      uuid <- POSIX.genUUID[S].liftM[ST]
      v = Version(uuid)

      back <- if (log.versions.contains(v)) {
        fresh[S]
      } else {
        for {
          _ <- putS[Free[S, ?], VersionLog](log.copy(versions = log.versions + v))
          target <- dirForVersion[Free[S, ?]](v)
          _ <- POSIX.mkDir[S](target).liftM[ST]
        } yield v
      }
    } yield back
  }

  def dirForVersion[F[_]: Monad](v: Version): StateT[F, VersionLog, ADir] =
    getS[F, VersionLog].map(_.baseDir </> Path.dir(v.value.toString))

  def commit[S[_]](v: Version)(implicit IP: POSIXOp :<: S, IT: Task :<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      log <- getS[Free[S, ?], VersionLog]
      log2 = log.copy(committed = v :: log.committed)

      _ <- if (log.versions.contains(v)) {
        for {
          _ <- putS[Free[S, ?], VersionLog](log2)

          vnew <- POSIX.openW[S](log.baseDir </> VersionsJsonNew).liftM[ST]

          json = log2.committed.asJson.nospaces
          // TODO character encoding!
          writer = Stream.emit(ByteVector(json.getBytes)).to(vnew).run
          _ <- POSIXWithTask.generalize(writer).liftM[ST]

          _ <- POSIX.move(log.baseDir </> VersionsJsonNew, log.baseDir </> VersionsJson).liftM[ST]
        } yield ()
      } else {
        ().point[StateT[Free[S, ?], VersionLog, ?]]
      }
    } yield ()
  }

  def headDir[F[_]: Monad]: StateT[F, VersionLog, Option[ADir]] = {
    for {
      log <- getS[F, VersionLog]

      back <- log.head.traverse(dirForVersion[F](_))
    } yield back
  }

  def purgeOld[S[_]](implicit I: POSIXOp :<: S): StateT[Free[S, ?], VersionLog, Unit] = {
    for {
      log <- getS[Free[S, ?], VersionLog]
      toPurge = log.committed.drop(KeepLimit)

      _ <- toPurge traverse { v =>
        for {
          dir <- dirForVersion[Free[S, ?]](v)
          _ <- POSIX.delete[S](dir).liftM[ST]
        } yield ()
      }

      log2 = log.copy(
        committed = log.committed.take(KeepLimit),
        versions = log.versions -- toPurge)

      _ <- putS[Free[S, ?], VersionLog](log2)
    } yield ()
  }

  // how is this not a member of StateT???
  private def putS[F[_]: Monad, S](s: S): StateT[F, S, Unit] =
    StateT[F, S, Unit](_ => (s, ()).point[F])

  // ditto
  private def getS[F[_]: Monad, S]: StateT[F, S, S] =
    StateT[F, S, S](s => (s, s).point[F])

  implicit def catchableForS[S[_]](implicit I: Task :<: S): Catchable[Free[S, ?]] =
    new Catchable[Free[S, ?]] {

      def pure[A](a: A): Free[S, A] = a.point[Free[S, ?]]

      def fail[A](t: Throwable): Free[S, A] = Free.liftF(I.inj(Task.fail(t)))

      def attempt[A](fa: Free[S, A]): Free[S, Either[Throwable, A]] = {
        val et = fa.foldMap(λ[S ~> EitherT[Free[S, ?], Throwable, ?]] { sa =>
          val fse = I.prj(sa) match {
            case Some(ta) => Free.liftF(I.inj(ta.attempt))
            case None => Free.liftF(sa).map(_.right[Throwable])
          }

          EitherT(fse)
        })

        et.run.map(_.toEither)
      }

      def flatMap[A, B](fa: Free[S, A])(f: A => Free[S, B]): Free[S, B] =
        fa.flatMap(f)
    }
}
