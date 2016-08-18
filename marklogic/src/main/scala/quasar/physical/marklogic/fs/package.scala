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
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.impl.ReadStream
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef.DefErrT

import java.net.URI

import com.fasterxml.uuid._
import com.marklogic.xcc._
import eu.timepit.refined.auto._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import xcc.ChunkedResultSequence

  val FsType = FileSystemType("marklogic")

  type MLReadHandles[A] = KeyValueStore[ReadHandle, ReadStream[Task], A]
  type MLWriteHandles[A] = KeyValueStore[WriteHandle, Unit, A]
  type MLResultHandles[A] = KeyValueStore[ResultHandle, ChunkedResultSequence, A]

  type Eff[A]  = Coproduct[Task, Eff0, A]
  type Eff0[A] = Coproduct[ClientR, Eff1, A]
  type Eff1[A] = Coproduct[MonotonicSeq, Eff2, A]
  type Eff2[A] = Coproduct[MLReadHandles, Eff3, A]
  type Eff3[A] = Coproduct[MLWriteHandles, Eff4, A]
  type Eff4[A] = Coproduct[MLResultHandles, xcc.SessionIO, A]

  def createClient(uri: URI): Task[Client] = Task.delay {
    val ifaceAddr = Option(EthernetAddress.fromInterface)
    val uuidGen = ifaceAddr.fold(Generators.timeBasedGenerator)(Generators.timeBasedGenerator)
    Client(ContentSourceFactory.newContentSource(uri), uuidGen)
  }

  def inter(uri0: ConnectionUri): Task[(Eff ~> Task, Task[Unit])] = {
    val uri = new URI(uri0.value)

    (
      KeyValueStore.impl.empty[WriteHandle, Unit]                   |@|
      KeyValueStore.impl.empty[ReadHandle, ReadStream[Task]]        |@|
      KeyValueStore.impl.empty[ResultHandle, ChunkedResultSequence] |@|
      MonotonicSeq.fromZero                                         |@|
      createClient(uri)
    ) { (whandles, rhandles, qhandles, seq, client) =>

      val toTask =
        reflNT[Task]                           :+:
        Read.constant[Task, Client](client)    :+:
        seq                                    :+:
        rhandles                               :+:
        whandles                               :+:
        qhandles                               :+:
        xcc.runSessionIO(client.contentSource)

      (toTask, ().point[Task])
    }
  }

  def definition[S[_]](implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        lift(inter(uri).map { case (run, release) =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            mapSNT(injectNT[Task, S] compose run) compose interpretFileSystem(
              queryfile.interpret[Eff](10000L),
              readfile.interpret[Eff](10000L),
              writefile.interpret[Eff],
              managefile.interpret[Eff]),
            lift(release).into[S])
        }).into[S].liftM[DefErrT]
    }

  implicit val chunkedResultSequenceDataCursor: DataCursor[Task, ChunkedResultSequence] =
    new DataCursor[Task, ChunkedResultSequence] {
      def close(crs: ChunkedResultSequence) =
        crs.close.void

      def nextChunk(crs: ChunkedResultSequence) =
        crs.nextChunk.map(_.foldLeft(Vector[Data]())((ds, x) =>
          ds :+ xdmitem.toData(x)))
    }
}
