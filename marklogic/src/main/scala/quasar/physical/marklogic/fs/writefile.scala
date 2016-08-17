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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar._
import quasar.fp._
import quasar.fs._
import quasar.fs.FileSystemError._
import quasar.fs.PathError._
import quasar.effect.{Read, KeyValueStore, MonotonicSeq}
import quasar.physical.marklogic._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object writefile {

  implicit val coded = DataCodec.Precise

  def asString(data: Vector[Data]): Vector[String] = data.map(DataCodec.render(_).toOption).unite

  def interpret[S[_]](implicit
    S0:      Task :<: S,
    client:  Read.Ops[Client,S],
    cursors: KeyValueStore.Ops[WriteFile.WriteHandle, Unit, S],
    seq:     MonotonicSeq.Ops[S]
   ): WriteFile ~> Free[S,?] = new (WriteFile ~> Free[S, ?]) {
    def apply[A](fa: WriteFile[A]): Free[S, A] = fa match {
      case WriteFile.Open(file) =>
        val asDir = fileParent(file) </> dir(fileName(file).value)
        (for {
          id <- seq.next.liftM[FileSystemErrT]
          writeHandle = WriteFile.WriteHandle(file, id)
          _ <- cursors.put(writeHandle, ()).liftM[FileSystemErrT]
          _ <- Client.exists(asDir).liftM[FileSystemErrT].ifM(
            // No need to do anything if it exists
            ().pure[Free[S,?]].liftM[FileSystemErrT],
            // Else, create the directory
            EitherT(Client.createDir(asDir)).leftMap(κ(pathErr(pathExists(file)))))
        } yield writeHandle).run

      case WriteFile.Write(h, data) =>
        val asDir = fileParent(h.file) </> dir(fileName(h.file).value)
        cursors.get(h).isDefined.ifM(
          Client.writeInDir(asDir, asString(data)).as(Vector[FileSystemError]()),
          Vector(unknownWriteHandle(h)).pure[Free[S,?]])

      case WriteFile.Close(h) =>
        cursors.delete(h)
    }
  }
}
