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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._
import quasar.common.PhaseResultT
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fp._
import quasar.fs._
import quasar.physical.mongodb._, MongoDb._, WorkflowExecutor.WorkflowCursor

import com.mongodb.async.client.MongoClient
import matryoshka.data.Fix
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfileTypes {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type MongoQuery[C, A]    = QueryRT[MongoDbIO, C, A]
}

object queryfile extends QueryFileModule {
  import queryfileTypes._
  import QueryFile._
  import bsoncursor._

  def inConfigured[A](qf: QueryFile[A]): Configured[A] =
    MonadReader_[Configured, Config].asks(_.wfExec).map(interpret[BsonCursor]) >>=
      (i => toConfigured(i(qf)))

  def inBackend[A](qf: QueryFile[FileSystemError \/ A]): Backend[A] =
    MonadReader_[Backend, Config].asks(_.wfExec).map(interpret[BsonCursor]) >>=
      (i => toBackend(i(qf)))

  def executePlan(repr: Repr, out: AFile): Backend[AFile] = ???

  def evaluatePlan(repr: Repr): Backend[ResultHandle] = ???

  def more(h: ResultHandle): Backend[Vector[Data]] =
    inBackend(More(h))

  def close(h: ResultHandle): Configured[Unit] = inConfigured(Close(h))

  def explain(repr: Repr): Backend[String] = ???

  def listContents(dir: ADir): Backend[Set[PathSegment]] =
    inBackend(ListContents(dir))

  def fileExists(file: AFile): Configured[Boolean] =
    inConfigured(FileExists(file))

  def interpret[C]
    (execMongo: WorkflowExecutor[MongoDbIO, C])
    (implicit C: DataCursor[MongoDbIO, C])
      : QueryFile ~> MongoQuery[C, ?] =
    new QueryFileInterpreter(
      execMongo,
      MongoDbPlanner.plan[Fix, FileSystemErrT[PhaseResultT[MongoDbIO, ?], ?]])

  def run[C, S[_]](
    client: MongoClient,
    defDb: Option[DefaultDb]
  )(implicit
    S0: Task :<: S,
    S1: PhysErr :<: S
  ): Task[MongoQuery[C, ?] ~> Free[S, ?]] = {
    type MQ[A] = MongoQuery[C, A]
    type F[A]  = Free[S, A]

    def runMQ(ref: TaskRef[EvalState[C]]): MQ ~> F =
      new (MQ ~> F) {
        def apply[A](mq: MQ[A]) =
          mq.run((defDb, ref)).runF(client)
      }

    TaskRef((0L, Map.empty: ResultMap[C])) map runMQ
  }
}
