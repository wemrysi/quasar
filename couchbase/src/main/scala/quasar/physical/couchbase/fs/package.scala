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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.effect.{Failure, KeyValueStore, MonotonicSeq, Read, UUID}
import quasar.fp._, free._
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef.DefErrT
import quasar.physical.couchbase.common.Context

import com.couchbase.client.java.CouchbaseCluster
import org.http4s.Uri
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Injection? Parameterized queries could help but don't appear to handle bucket names within ``
// TODO: Handle query returned errors field

package object fs {
  val FsType = FileSystemType("couchbase")

  type Eff[A] = (
    Task                                           :\:
    UUID                                           :\:
    Read[Context, ?]                               :\:
    MonotonicSeq                                   :\:
    KeyValueStore[ReadHandle,  readfile.Cursor, ?] :/:
    KeyValueStore[WriteHandle, writefile.State, ?]
  )#M[A]

  // TODO: move away from Task failures

  def clusterManager(cluster: CouchbaseCluster, username: String, password: String): Task[Context] =
    Task.delay(
      Option(cluster.clusterManager(username, password)).cata(
        Task.now(_),
        Task.fail(new RuntimeException(
          "Unable to obtain a ClusterManager with provided credentials.")))
    ).join.map(Context(cluster, _))

  def interp[S[_]](
      connectionUri: ConnectionUri
    )(implicit
      S0: Task :<: S,
      failure: Failure.Ops[PhysicalError, S]
    ): Free[S, (Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])] = {

    val clusterMngr =
      for {
        uri     <- Task.fromDisjunction(Uri.fromString(connectionUri.value))
        cluster =  CouchbaseCluster.fromConnectionString(uri.renderString)
        user    <- Task.fromMaybe(uri.params.get("username").toMaybe)(
                     new RuntimeException("No username in ConnectionUri"))
        pass    <- Task.fromMaybe(uri.params.get("password").toMaybe)(
                     new RuntimeException("No password in ConnectionUri"))
        manager <- clusterManager(cluster, user, pass)
      } yield (cluster, manager)

    def taskInterp: Task[(Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])]  =
      (clusterMngr                                      |@|
       TaskRef(Map.empty[ReadHandle,  readfile.Cursor]) |@|
       TaskRef(Map.empty[WriteHandle, writefile.State]) |@|
       TaskRef(0L)
      )((cm, kvR, kvW, i) =>
      (
        mapSNT(injectNT[Task, S] compose (
          reflNT[Task]                          :+:
          UUID.toTask                           :+:
          Read.constant[Task, Context](cm._2)   :+:
          MonotonicSeq.fromTaskRef(i)           :+:
          KeyValueStore.impl.fromTaskRef(kvR)   :+:
          KeyValueStore.impl.fromTaskRef(kvW))),
        lift(Task.delay(cm._1.disconnect()).void).into
      ))

    lift(taskInterp).into
  }

  def definition[S[_]](implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        interp(uri).map { case (run, close) =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            run compose interpretFileSystem(
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            close)
        }.liftM[DefErrT]
    }
}
