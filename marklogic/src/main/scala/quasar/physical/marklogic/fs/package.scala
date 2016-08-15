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
import quasar.effect.{Failure, KeyValueStore, MonotonicSeq, Read}
import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef.DefErrT

import com.marklogic.client._
import com.marklogic.xcc._
import java.net.URI

import eu.timepit.refined.auto._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import xcc.ChunkedResultSequence

  val FsType = FileSystemType("marklogic")

  type XccCursor[A]  = Coproduct[Task, xcc.XccFailure, A]
  type XccCursorM[A] = Free[XccCursor, A]

  type MLReadHandles[A] = KeyValueStore[ReadHandle, Process[Task, Vector[Data]], A]
  type MLWriteHandles[A] = KeyValueStore[WriteHandle, Unit, A]
  type MLResultHandles[A] = KeyValueStore[ResultHandle, ChunkedResultSequence[XccCursor], A]

  type Eff[A]  = Coproduct[Task, Eff0, A]
  type Eff0[A] = Coproduct[ClientR, Eff1, A]
  type Eff1[A] = Coproduct[MonotonicSeq, Eff2, A]
  type Eff2[A] = Coproduct[MLReadHandles, Eff3, A]
  type Eff3[A] = Coproduct[MLWriteHandles, Eff4, A]
  type Eff4[A] = Coproduct[MLResultHandles, Eff5, A]
  type Eff5[A] = Coproduct[xcc.SessionR, Eff6, A]
  type Eff6[A] = Coproduct[xcc.XccFailure, XccCursorM, A]

  def createClient(uri: URI): Task[Client] = Task.delay {
    val (user, password0) = uri.getUserInfo.span(_ ≠ ':')
    val password = password0.drop(1)
    def dbClient = DatabaseClientFactory.newClient(
      uri.getHost,
      uri.getPort,
      user,
      password,
      DatabaseClientFactory.Authentication.DIGEST)
    Client(dbClient, ContentSourceFactory.newContentSource(uri))
  }

  def inter(uri0: ConnectionUri): Task[(Eff ~> Task, Task[Unit])] = {
    val uri = new URI(uri0.value)

    val failErrs = Failure.toRuntimeError[Task, xcc.XccError]

    (
      KeyValueStore.impl.empty[WriteHandle, Unit]                              |@|
      KeyValueStore.impl.empty[ReadHandle, Process[Task, Vector[Data]]]        |@|
      KeyValueStore.impl.empty[ResultHandle, ChunkedResultSequence[XccCursor]] |@|
      MonotonicSeq.fromZero                                                    |@|
      createClient(uri)
    ) { (whandles, rhandles, qhandles, seq, client) =>

      // TODO: Define elsewhere, can probably make this another generic impl of Read
      val newSession: xcc.SessionR ~> Task =
      new (xcc.SessionR ~> Task) {
        def apply[A](ra: Read[Session, A]) = ra match {
          case Read.Ask(f) => client.newSession.map(f)
        }
      }

      val toTask =
        reflNT[Task]                        :+:
        Read.constant[Task, Client](client) :+:
        seq                                 :+:
        rhandles                            :+:
        whandles                            :+:
        qhandles                            :+:
        newSession                          :+:
        failErrs                            :+:
        foldMapNT(reflNT[Task] :+: failErrs)

      (toTask, client.release)
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
              queryfile.interpret[Eff](100L),
              readfile.interpret[Eff],
              writefile.interpret[Eff],
              managefile.interpret[Eff]),
            lift(release).into[S])
        }).into[S].liftM[DefErrT]
    }

}
