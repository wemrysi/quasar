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
import quasar.effect.{Failure, KeyValueStore, MonotonicSeq, Read}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
import quasar.fs.mount._, FileSystemDef.DefErrT
import quasar.physical.couchbase.common.{Context, Cursor}

import com.couchbase.client.java.CouchbaseCluster
import org.http4s.Uri
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Injection? Parameterized queries could help but don't appear to handle bucket names within ``
// TODO: Handle query returned errors field

package object fs {
  val FsType = FileSystemType("couchbase")

  type Eff[A] = (
    Task                                             :\:
    Read[Context, ?]                                 :\:
    MonotonicSeq                                     :\:
    GenUUID                                          :\:
    KeyValueStore[ReadHandle,   Cursor,  ?]          :\:
    KeyValueStore[WriteHandle,  writefile.State,  ?] :/:
    KeyValueStore[ResultHandle, Cursor, ?]
  )#M[A]

  def physErr(msg: String): PhysicalError = unhandledFSError(new RuntimeException(msg))

  def interp[S[_]](
      connectionUri: ConnectionUri
    )(implicit
      S0: Task :<: S,
      failure: Failure.Ops[PhysicalError, S]
    ): Free[S, (Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])] = {

    val cbCtx: EitherT[Task, PhysicalError, Context] =
      for {
        uri     <- EitherT.fromDisjunction[Task](
                     Uri.fromString(connectionUri.value).leftMap(e => physErr(e.message))
                   )
        cluster <- EitherT(Task.delay(
                     CouchbaseCluster.fromConnectionString(uri.renderString).right
                   ).handle { case e: Exception => unhandledFSError(e).left })
        user    <- EitherT.fromDisjunction[Task](
                     uri.params.get("username") \/> physErr("No username in ConnectionUri")
                   )
        pass    <- EitherT.fromDisjunction[Task](
                     uri.params.get("password") \/> physErr("No password in ConnectionUri")
                   )
        ctx     <- EitherT(Task.delay(
                     Option(cluster.clusterManager(user, pass)) \/>
                       physErr("Unable to obtain a ClusterManager with provided credentials.")
                   )).map(Context(cluster, _))
      } yield ctx

    def taskInterp(
      ctx: Context
    ): Task[(Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])]  =
      (TaskRef(Map.empty[ReadHandle,   Cursor])          |@|
       TaskRef(Map.empty[WriteHandle,  writefile.State]) |@|
       TaskRef(Map.empty[ResultHandle, Cursor])          |@|
       TaskRef(0L)                                       |@|
       GenUUID.type1
     )((kvR, kvW, kvQ, i, genUUID) =>
      (
        mapSNT(injectNT[Task, S] compose (
          reflNT[Task]                          :+:
          Read.constant[Task, Context](ctx)     :+:
          MonotonicSeq.fromTaskRef(i)           :+:
          genUUID                               :+:
          KeyValueStore.impl.fromTaskRef(kvR)   :+:
          KeyValueStore.impl.fromTaskRef(kvW)   :+:
          KeyValueStore.impl.fromTaskRef(kvQ))),
        lift(Task.delay(ctx.cluster.disconnect()).void).into
      ))

    lift(cbCtx.run).into >>= (_.fold(
      failure.fail,
      ctx => lift(taskInterp(ctx)).into
    ))
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
