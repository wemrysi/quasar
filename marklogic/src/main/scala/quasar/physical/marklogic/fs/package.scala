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
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef.DefErrT

import com.marklogic.client._
import com.marklogic.xcc._
import java.net.URI

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

package object fs {
  val FsType = FileSystemType("marklogic")

  type DataStream = Process[Task, Vector[Data]]

  type Eff0[A] = Coproduct[
                    KeyValueStore[ReadFile.ReadHandle, DataStream, ?],
                    KeyValueStore[WriteFile.WriteHandle, Unit, ?],
                    A]
  type Eff1[A] = Coproduct[MonotonicSeq, Eff0, A]
  type Eff2[A] = Coproduct[Read[Client, ?], Eff1, A]
  type Eff[A]  = Coproduct[Task, Eff2, A]

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
    (KeyValueStore.impl.empty[WriteFile.WriteHandle, Unit]     |@|
     KeyValueStore.impl.empty[ReadFile.ReadHandle, DataStream] |@|
     MonotonicSeq.fromZero                                     |@|
     createClient(uri)                                         )((a,b,c,client) =>
       (reflNT[Task] :+: Read.constant[Task, Client](client) :+: c :+: b :+: a, client.release))
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
              queryfile.interpret[Eff],
              readfile.interpret[Eff],
              writefile.interpret[Eff],
              managefile.interpret[Eff]),
            lift(release).into[S])
        }).into[S].liftM[DefErrT]
    }

}
