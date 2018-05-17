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

package quasar.mimir

import quasar.Data
import quasar.contrib.pathy.{ADir, AFile, PathSegment}
import quasar.fp.PrismNT
import quasar.fs.{FileSystem, Local, QueryFile, ReadFile}
import quasar.fs.mount.ConnectionUri

import eu.timepit.refined.auto._
import fs2.Stream
import scalaz.{:<:, EitherT, OptionT}
import scalaz.Scalaz._
import scalaz.concurrent.Task

object LightweightLocalFileSystem extends LightweightFileSystem {

  def children(dir: ADir): Task[Option[Set[PathSegment]]] =
    for {
      interp <- Local.runFs
      back <- interp.apply(toFS(QueryFile.ListContents(dir)))
    } yield back.toOption.map(_.map(_.segment))

  def exists(file: AFile): Task[Boolean] =
    for {
      interp <- Local.runFs
      back <- interp.apply(toFS(QueryFile.FileExists(file)))
    } yield back

  def read(file: AFile): Task[Option[Stream[Task, Data]]] = {
    val back: OptionT[Task, Stream[Task, Data]] = for {
      interp <- Local.runFs.liftM[OptionT]
      handle <- OptionT.optionT {
        interp.apply(toFS(ReadFile.Open(file, 0L, None))).map(_.toOption)
      }
      data <- OptionT.optionT {
        interp.apply(toFS(ReadFile.Read(handle))).map(_.toOption)
      }
    } yield {
      // TODO don't create this manually
      implicit val fs2ApplicativeTask = new fs2.util.Applicative[Task] {
        def pure[A](a: A): Task[A] = scalaz.Applicative[Task].pure(a)
        def ap[A,B](fa: Task[A])(f: Task[A => B]): Task[B] = scalaz.Apply[Task].ap(fa)(f)
        def map[A,B](a: Task[A])(f: A => B): Task[B] = scalaz.Functor[Task].map(a)(f)
      }

      val close: Task[Unit] =
        interp.apply(toFS(ReadFile.Close(handle)))

      Stream.apply[Task, Data](data: _*).onFinalize(close)
    }

    back.run
  }

  ////////

  private def toFS[A, FS[_]](fs: FS[A])(implicit I: FS :<: FileSystem): FileSystem[A] =
    PrismNT.inject[FS, FileSystem].apply(fs)
}

object LightweightLocal extends LightweightConnector {
  def init(uri: ConnectionUri): EitherT[Task, String, (LightweightFileSystem, Task[Unit])] =
    EitherT.rightT(Task.delay {
      (LightweightLocalFileSystem, Task.now(()))
    })
}
