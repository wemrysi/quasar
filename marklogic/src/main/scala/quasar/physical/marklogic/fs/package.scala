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

package quasar.physical.marklogic

import quasar.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp._, free._
import quasar.fs._, impl.ReadStream
import quasar.fs.mount._, FileSystemDef.DefErrT

import java.net.URI

import com.marklogic.xcc._
import eu.timepit.refined.auto._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import xcc.{ContentSourceIO, ResultCursor, SessionIO}
  import uuid.GenUUID

  type MLReadHandles[A] = KeyValueStore[ReadHandle, ReadStream[ContentSourceIO], A]
  type MLWriteHandles[A] = KeyValueStore[WriteHandle, Unit, A]
  type MLResultHandles[A] = KeyValueStore[ResultHandle, ResultCursor, A]

  type MarkLogicFs[A] = (
        Task
    :\: SessionIO
    :\: ContentSourceIO
    :\: GenUUID
    :\: MonotonicSeq
    :\: MLReadHandles
    :\: MLWriteHandles
    :/: MLResultHandles
  )#M[A]

  val FsType = FileSystemType("marklogic")

  def definition[S[_]](
    implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case FsCfg(FsType, uri) =>
        lift(runMarkLogicFs(uri).map { case (run, shutdown) =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            mapSNT(injectNT[Task, S] compose run) compose interpretFileSystem(
              queryfile.interpret[MarkLogicFs](10000L),
              readfile.interpret[MarkLogicFs](10000L),
              writefile.interpret[MarkLogicFs],
              managefile.interpret[MarkLogicFs]),
            lift(shutdown).into[S])
        }).into[S].liftM[DefErrT]
    }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def runMarkLogicFs(connectionUri: ConnectionUri): Task[(MarkLogicFs ~> Task, Task[Unit])] = {
    val uri = new URI(connectionUri.value)


    (
      KeyValueStore.impl.empty[WriteHandle, Unit]                       |@|
      KeyValueStore.impl.empty[ReadHandle, ReadStream[ContentSourceIO]] |@|
      KeyValueStore.impl.empty[ResultHandle, ResultCursor]              |@|
      MonotonicSeq.fromZero                                             |@|
      GenUUID.type1                                                     |@|
      // TODO: Catch any XccConfigExceptions thrown here and returns as config errors
      Task.delay(ContentSourceFactory.newContentSource(uri))
    ) { (whandles, rhandles, qhandles, seq, genUUID, csource) =>
      val runCSIO = ContentSourceIO.runNT(csource)
      val runSIO  = runCSIO compose ContentSourceIO.runSessionIO

      val runML = reflNT[Task] :+: runSIO :+: runCSIO :+: genUUID :+: seq :+: rhandles :+: whandles :+: qhandles

      (runML, Task.delay(csource.getConnectionProvider.shutdown(null)))
    }
  }

  implicit val resultCursorDataCursor: DataCursor[Task, ResultCursor] =
    new DataCursor[Task, ResultCursor] {
      def close(rc: ResultCursor) =
        rc.close.void

      def nextChunk(rc: ResultCursor) =
        TV.map(rc.nextChunk)(xdm => xdmitem.toData[ErrorMessages \/ ?](xdm) | Data.NA)

      val TV = Functor[Task].compose[Vector]
    }

  def asDir(file: AFile): ADir =
    fileParent(file) </> dir(fileName(file).value)

  def pathUri(path: APath): String =
    posixCodec.printPath(path)
}
