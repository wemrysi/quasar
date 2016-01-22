/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.physical.mongodb

import quasar.Predef._
import quasar.{EnvironmentError2, EnvErr2T, EnvErrF}
import quasar.effect.Failure
import quasar.fp._
import quasar.fs.{Path => _, _}
import quasar.physical.mongodb.fs.bsoncursor._

import com.mongodb.async.client.MongoClient
import scalaz.{Free, Functor, Hoist, ~>, :<:}
import scalaz.syntax.monad._
import scalaz.concurrent.Task

package object fs {
  type WFTask[A] = WorkflowExecErrT[Task, A]

  val MongoDBFsType = FileSystemType("mongodb")

  final case class DefaultDb(run: String) extends scala.AnyVal

  object DefaultDb {
    def fromPath(path: APath): Option[DefaultDb] =
      Collection.dbNameFromPath(path).map(DefaultDb(_)).toOption
  }

  final case class TmpPrefix(run: String) extends scala.AnyVal

  def mongoDbFileSystem(
    client: MongoClient,
    defDb: Option[DefaultDb]
  ): EnvErr2T[Task, FileSystem ~> WFTask] = {
    val liftWF = liftMT[Task, WorkflowExecErrT]
    val runM = Hoist[EnvErr2T].hoist(MongoDbIO.runNT(client))

    (
      runM(WorkflowExecutor.mongoDb)                 |@|
      queryfile.run[BsonCursor](client, defDb)
        .liftM[EnvErr2T]                             |@|
      readfile.run(client).liftM[EnvErr2T]           |@|
      writefile.run(client).liftM[EnvErr2T]          |@|
      managefile.run(client).liftM[EnvErr2T]
    )((execMongo, qfile, rfile, wfile, mfile) =>
      interpretFileSystem[WFTask](
        qfile compose queryfile.interpret(execMongo),
        liftWF compose rfile compose readfile.interpret,
        liftWF compose wfile compose writefile.interpret,
        liftWF compose mfile compose managefile.interpret))
  }

  /** TODO: Refactor MongoDB interpreters to interpret into these (and other) effects. */
  def mongoDbFileSystemF[S[_]: Functor](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: EnvErrF :<: S,
    S2: WorkflowExecErrF :<: S
  ): Free[S, FileSystem ~> Free[S, ?]] = {
    type M[A] = Free[S, A]

    val envErr = Failure.Ops[EnvironmentError2, S]
    val wfeErr = Failure.Ops[WorkflowExecutionError, S]
    def liftT[A](ta: Task[A]): M[A] = free.lift(ta).into[S]

    val toS: WFTask ~> M =
      new (WFTask ~> M) {
        def apply[A](wf: WFTask[A]) =
          liftT(wf.run).flatMap(_.fold(wfeErr.fail, _.point[M]))
      }

    liftT(mongoDbFileSystem(client, defDb).run)
      .flatMap(_.fold(envErr.fail, f => (toS compose f).point[M]))
  }
}
