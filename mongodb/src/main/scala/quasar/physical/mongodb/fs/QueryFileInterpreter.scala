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
import quasar._, RenderTree.ops._
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.kleisli._
import quasar.fp._
import quasar.fp.ski._
import quasar.fs._
import quasar.javascript._
import quasar.physical.mongodb._, MongoDb._, WorkflowExecutor.WorkflowCursor

import argonaut.JsonObject, JsonObject.{single => jSingle}
import argonaut.JsonIdentity._
import java.time.Instant
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class QueryFileInterpreter(execMongo: WorkflowExecutor[MongoDbIO, BsonCursor]) {

  import QueryFile._
  import quasar.physical.mongodb.workflow._
  import FileSystemError._
  import queryfileTypes._
  import QueryContext._

  private val execJs = WorkflowExecutor.javaScript

  def execPlan(repr: Repr, out: AFile): Backend[AFile] = {
    val x: MongoLogWFR[C, AFile] = for {
      dst <- EitherT(Collection.fromFile(out)
                .leftMap(pathErr(_))
                .point[MongoLogWF[C, ?]])
      coll <- handlePlan(repr, execJs.execute(_, dst), execWorkflow(_, dst, _))
    } yield coll.asFile

    toBackendP(x.run.run)
  }

  def evalPlan(repr: Repr, dbName: Option[DatabaseName]): Backend[ResultHandle] = {
    val x: MongoLogWFR[C, ResultHandle] = for {
      rcursor <- handlePlan(repr, execJs.evaluate(_, dbName), evalWorkflow(_, dbName, _))
      handle <- liftMQ(recordCursor(rcursor))
    } yield handle

    toBackendP(x.run.run)
  }

  def explain(repr: Repr, dbName: Option[DatabaseName]): Backend[String] = {
    val (stmts, r) = execJs.evaluate(repr, dbName)
                       .leftMap(wfErrToFsErr(repr))
                       .run.run(CollectionName("tmp.gen_"))
                       .eval(0).run
    val out = Js.Stmts(stmts.toList).pprint(0)

    val x: MongoLogWFR[C, String] = for {
      exp <- EitherT.fromDisjunction[MongoLogWF[C, ?]](r.as(out))
      _   <- logProgram(stmts).liftM[FileSystemErrT]
    } yield exp

    toBackendP(x.run.run)
  }

  def more(h: ResultHandle): Backend[Vector[Data]] = {
    val m = moreResults(h)
      .toRight(unknownResultHandle(h))
      .run

    toBackend(m)
  }

  def close(h: ResultHandle): Configured[Unit] = {
    val c = OptionT[MQ, ResultCursor[C]](MongoQuery(resultsL(h) <:= none))
      .flatMapF(_.fold(κ(().point[MQ]), wc =>
        DataCursor[MongoDbIO, WorkflowCursor[C]]
          .close(wc)
          .liftM[QRT]))
      .run.void

    toConfigured(c)
  }

  def listContents0(dir: ADir): Backend[Set[PathSegment]] = {
    val c = listContents(dir).run.liftM[QRT]

    toBackend(c)
  }

  def fileExists(file: AFile): Configured[Boolean] = {
    val e = Collection.fromFile(file).fold(
      κ(false.point[MQ]),
      coll => MongoDbIO.collectionExists(coll).liftM[QRT])

    toConfigured(e)
  }

  def queryTime: Backend[Instant] = {
    val n = MongoDbIO.liftTask(Task.delay { Instant.now }).liftM[QRT]
    toBackendP(liftMQ(n).run.run)
  }

  ////

  private type PlanR[A]       = EitherT[WriterT[MongoDbIO, PhaseResults, ?], FileSystemError, A]

  type C = BsonCursor
  implicit val DC: DataCursor[MongoDbIO, C] = bsoncursor.bsonCursorDataCursor

  private type JsR[A] =
    WorkflowExecErrT[ReaderT[StateT[JavaScriptLog, Long, ?], CollectionName, ?], A]

  private val queryR =
    MonadReader[MQ, (Option[DefaultDb], TaskRef[EvalState[C]])]

  // FIXME: Not sure how to distinguish these.
  @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
  private def MongoQuery[A](f: TaskRef[EvalState[C]] => Task[A]): MQ[A] =
    queryR.ask flatMapK { case (_, ref) => MongoDbIO.liftTask(f(ref)) }

  private def MongoQuery[A](s: State[EvalState[C], A]): MQ[A] =
    MongoQuery(_ modifyS s.run)

  private val seqL: EvalState[C] @> Long =
    Lens.firstLens

  private val resultMapL: EvalState[C] @> ResultMap[C] =
    Lens.secondLens

  private def resultsL(h: ResultHandle): EvalState[C] @> Option[ResultCursor[C]] =
    Lens.mapVLens(h) <=< resultMapL

  private def freshHandle: MQ[ResultHandle] =
    MongoQuery(seqL <%= (_ + 1)) map (ResultHandle(_))

  private def recordCursor(c: ResultCursor[C]): MQ[ResultHandle] =
    freshHandle flatMap (h => MongoQuery(resultsL(h) := some(c)) as h)

  private def lookupCursor(h: ResultHandle): OptionT[MQ, ResultCursor[C]] =
    OptionT(MongoQuery(resultsL(h).st))

  private def defaultDbName: MQ[Option[DatabaseName]] =
    queryR.asks(_._1.map(_.run))

  private def genPrefix: MQ[CollectionName] =
    MongoDbIO.liftTask(NameGenerator.salt)
      .map(salt => CollectionName(s"tmp.gen_${salt}_"))
      .liftM[QRT]

  private val liftMQ: MQ ~> MongoLogWFR[C, ?] =
    liftMT[MongoLogWF[C, ?], FileSystemErrT] compose liftMT[MQ, PhaseResultT]

  def handlePlan[A](
    repr: Repr,
    log: Repr => JsR[_],
    handle: (Repr, CollectionName) => WorkflowExecErrT[MQ, A]
  ): MongoLogWFR[C, A] =
    for {
      prefix <- liftMQ(genPrefix)
      _ <- writeJsLog(repr, log(repr), prefix)
      a <- toMongoLogWFR(repr, handle(repr, prefix))
    } yield a

  private def toMongoLogWFR[A](repr: Repr, wf: WorkflowExecErrT[MQ, A]): MongoLogWFR[C, A] =
    EitherT[MongoLogWF[C, ?], FileSystemError, A](
                wf.leftMap(wfErrToFsErr(repr))
                  .run.mapK(_.attemptMongo.leftMap(err =>
                    executionFailed0(
                      s"MongoDB Error: ${err.cause.getMessage}",
                      JsonObject.empty,
                      some(err)).left[A]
                    ).merge)
                  .liftM[PhaseResultT])

  def execWorkflow(
    wf: Repr,
    dst: Collection,
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, Collection] =
    EitherT[MQ, WorkflowExecutionError, Collection](
      execMongo.execute(wf, dst).run.run(tmpPrefix).eval(0).liftM[QRT])

  def evalWorkflow(
    wf: Repr,
    defDb: Option[DatabaseName],
    tmpPrefix: CollectionName
  ): WorkflowExecErrT[MQ, ResultCursor[C]] =
    EitherT[MQ, WorkflowExecutionError, ResultCursor[C]](
      execMongo.evaluate(wf, defDb).run.run(tmpPrefix).eval(0).liftM[QRT])

  private def writeJsLog(repr: Repr, jsr: JsR[_], tmpPrefix: CollectionName): MongoLogWFR[C, Unit] = {
    val (stmts, r) = jsr.run.run(tmpPrefix).eval(0).run
    EitherT(logProgram(stmts) as r.leftMap(wfErrToFsErr(repr))).void
  }

  private def logProgram(prog: JavaScriptPrg): MongoLogWF[C, Unit] =
    MonadTell[MongoLogWF[C, ?], PhaseResults].tell(Vector(
      PhaseResult.detail("MongoDB", Js.Stmts(prog.toList).pprint(0))))

  private def moreResults(h: ResultHandle): OptionT[MQ, Vector[Data]] = {
    def pureNextChunk(bsons: List[Bson]) =
      if (bsons.isEmpty)
        Vector.empty[Data].point[MQ]
      else
        MongoQuery(resultsL(h) := some(List().left))
          .as(bsons.map(BsonCodec.toData).toVector)

    lookupCursor(h) flatMapF (_.fold(pureNextChunk, wc =>
      DataCursor[MongoDbIO, WorkflowCursor[C]]
        .nextChunk(wc)
        .liftM[QRT]))
  }

  import WorkflowExecutionError.{InvalidTask, InsertFailed, NoDatabase}

  private def wfErrToFsErr(repr: Repr): WorkflowExecutionError => FileSystemError = {
    case InvalidTask(task, reason) =>
      executionFailed0(
        s"Invalid MongoDB workflow task: $reason",
        jSingle("workflowTask", task.render.asJson),
        none)

    case InsertFailed(bson, reason) =>
      executionFailed0(
        s"Unable to insert data into MongoDB: $reason",
        jSingle("data", bson.shows.asJson),
        none)

    case NoDatabase =>
      executionFailed0_(
        "Executing this plan on MongoDB requires temporary collections, but a database in which to store them could not be determined.")
  }
}
