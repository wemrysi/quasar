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
import quasar.{config => _, _}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fp._
import quasar.fs._
import quasar.physical.mongodb._, MongoDb._, WorkflowExecutor.WorkflowCursor

import com.mongodb.async.client.MongoClient
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
}

object queryfile extends QueryFileModule {
  import queryfileTypes._
  import QueryFile._

  private def mkInterp(cfg: Config): QueryFileInterpreter =
    new QueryFileInterpreter(cfg.wfExec)

  def executePlan(repr: Repr, out: AFile): Backend[AFile] =
    for {
      i <- config[Backend].map(mkInterp)
      dst <- i.execPlan(repr, out)
    } yield dst

  def evaluatePlan(repr: Repr): Backend[ResultHandle] =
    for {
      dbName <- config[Backend].map(_.defaultDb.map(_.run))
      i <- config[Backend].map(mkInterp)
      handle <- i.evalPlan(repr, dbName)
    } yield handle

  def explain(repr: Repr): Backend[String] =
    for {
      dbName <- config[Backend].map(_.defaultDb.map(_.run))
      i <- config[Backend].map(mkInterp)
      s <- i.explain(repr, dbName)
    } yield s

  def more(h: ResultHandle): Backend[Vector[Data]] =
    for {
      i <- config[Backend].map(mkInterp)
      d <- i.more(h)
    } yield d

  def close(h: ResultHandle): Configured[Unit] =
    for {
      i <- config[Configured].map(mkInterp)
      u <- i.close(h)
    } yield u

  def listContents(dir: ADir): Backend[Set[PathSegment]] =
    for {
      i <- config[Backend].map(mkInterp)
      c <- i.listContents0(dir)
    } yield c

  def fileExists(file: AFile): Configured[Boolean] =
    for {
      i <- config[Configured].map(mkInterp)
      c <- i.fileExists(file)
    } yield c

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
