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
import quasar.common.PhaseResults
import quasar.{config => _, _}
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fp._
import quasar.fs._
import quasar.physical.mongodb._, MongoDb._, WorkflowExecutor.WorkflowCursor

import com.mongodb.async.client.MongoClient
import java.time.Instant
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfileTypes {
  import QueryFile.ResultHandle

  type ResultCursor[C]     = List[Bson] \/ WorkflowCursor[C]
  type ResultMap[C]        = Map[ResultHandle, ResultCursor[C]]
  type EvalState[C]        = (Long, ResultMap[C])
  type QueryRT[F[_], C, A] = ReaderT[F, (Option[DefaultDb], TaskRef[EvalState[C]]), A]
  type MongoQuery[C, A]    = QueryRT[MongoDbIO, C, A]

  type QRT[F[_], A]        = QueryRT[F, BsonCursor, A]
  type MQ[A]               = QRT[MongoDbIO, A]
  type MQErr[A]            = MQ[FileSystemError \/ A]
  type MQPhErr[A]          = MQ[(PhaseResults, FileSystemError \/ A)]
}

object queryfile extends QueryFileModule {
  import queryfileTypes._
  import QueryFile._

  private def mkInterp[F[_] : Functor](implicit C: MonadReader_[F, Config]): F[QueryFileInterpreter] =
    config[F].map(cfg => new QueryFileInterpreter(cfg.wfExec))

  def executePlan(repr: Repr, out: AFile): Backend[AFile] =
    mkInterp[Backend] >>= (i => toBackendP(i.execPlan(repr, out)))

  def evaluatePlan(repr: Repr): Backend[ResultHandle] =
    for {
      dbName <- config[Backend].map(_.defaultDb.map(_.run))
      i <- mkInterp[Backend]
      handle <- toBackendP(i.evalPlan(repr, dbName))
    } yield handle

  def explain(repr: Repr): Backend[String] =
    for {
      dbName <- config[Backend].map(_.defaultDb.map(_.run))
      i <- mkInterp[Backend]
      s <- toBackendP(i.explain(repr, dbName))
    } yield s

  def more(h: ResultHandle): Backend[Vector[Data]] =
    mkInterp[Backend] >>= (i => toBackend(i.more(h)))

  def close(h: ResultHandle): Configured[Unit] =
    mkInterp[Configured] >>= (i => toConfigured(i.close(h)))

  def listContents(dir: ADir): Backend[Set[PathSegment]] =
    mkInterp[Backend] >>= (i => toBackend(i.listContents0(dir)))

  def fileExists(file: AFile): Configured[Boolean] =
    mkInterp[Configured] >>= (i => toConfigured(i.fileExists(file)))

  def queryTime: Configured[Instant] =
    mkInterp[Configured] >>= (i => toConfigured(i.queryTime))

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
