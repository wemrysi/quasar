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

package quasar
package physical.mongodb

import slamdata.Predef._
import quasar.common._
import quasar.connector._
import quasar.contrib.pathy._
import quasar.effect.{Kvs, MonoSeq}
import quasar.fp.numeric._
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.mount._
import quasar.physical.mongodb.fs.bsoncursor._
import quasar.physical.mongodb.workflow._
import quasar.qscript._

import java.time.Instant
import matryoshka._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly

object MongoDb
    extends BackendModule
    with ManagedReadFile[BsonCursor]
    with ManagedWriteFile[Collection] {

  type QS[T[_[_]]] = fs.MongoQScriptCP[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  type Repr = Crystallized[WorkflowF]

  type M[A] = fs.MongoM[A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = fs.MongoConfig

  // Managed
  def MonoSeqM = MonoSeq[M]
  def ReadKvsM = Kvs[M, ReadFile.ReadHandle, BsonCursor]
  def WriteKvsM = Kvs[M, WriteFile.WriteHandle, Collection]

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] =
    fs.parseConfig(uri)

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    fs.compile(cfg)

  val Type = FileSystemType("mongodb")

  private def checkPathsExist[T[_[_]]: BirecursiveT](qs: T[MongoDb.QSM[T, ?]]): Backend[Unit] = {
    import fs.QueryContext._, fs.queryfileTypes.QRT
    val rez = for {
      colls <- EitherT.fromDisjunction[MongoDbIO](
                 fs.QueryContext.collections(qs).leftMap(FileSystemError.pathErr(_)))
      _     <- colls.traverse_(c => EitherT(MongoDbIO.collectionExists(c)
                .map(_ either (()) or FileSystemError.pathErr(PathError.pathNotFound(c.asFile)))))
    } yield ()
    val e: MongoLogWFR[BsonCursor, Unit] = EitherT[MongoLogWF[BsonCursor, ?], FileSystemError, Unit](
      rez.run.liftM[QRT].liftM[PhaseResultT])

    toBackendP(e.run.run)
  }

  def doPlan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      N[_]: Monad: MonadFsErr: PhaseResultTell]
      (qs: T[QSM[T, ?]], ctx: fs.QueryContext[N], execTime: Instant): N[Repr] =
      MongoDbPlanner.planExecTime[T, N](qs, ctx, execTime)

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qs: T[QSM[T, ?]]): Backend[Repr] =
    for {
      ctx <- toBackendP(fs.QueryContext.queryContext[T, Backend](qs, fs.queryfile.listContents))
      _ <- checkPathsExist(qs)
      execTime <- fs.queryfile.queryTime.liftM[PhaseResultT].liftM[FileSystemErrT]
      p <- doPlan[T, Backend](qs, ctx, execTime)
    } yield p

  private type PhaseRes[A] = PhaseResultT[ConfiguredT[M, ?], A]

  private val effToConfigured: fs.Eff ~> Configured =
    λ[fs.Eff ~> Configured](eff => Free.liftF(eff).liftM[ConfiguredT])

  private val effToPhaseRes: fs.Eff ~> PhaseRes =
    λ[Configured ~> PhaseRes](_.liftM[PhaseResultT]) compose effToConfigured

  private def toEff[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): fs.Eff[A] = inj(c)

  def toM[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): M[A] = Free.liftF(toEff(c))

  def toBackend[C[_], A](c: C[FileSystemError \/ A])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(c).mapT(x => effToPhaseRes(toEff(x)))

  def toBackendP[C[_], A](c: C[(PhaseResults, FileSystemError \/ A)])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(WriterT(effToConfigured(toEff(c))))

  def toConfigured[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): Configured[A] =
    effToConfigured(toEff(c))

  val DC = DataCursor[MongoDbIO, BsonCursor]

  override val QueryFileModule = fs.queryfile

  object ManagedReadFileModule extends ManagedReadFileModule {

    private def cursor(coll: Collection, offset: Natural, limit: Option[Positive]): MongoDbIO[BsonCursor] =
      for {
        iter <- MongoDbIO.find(coll)
        iter2 =  iter.skip(offset.value.toInt)
        iter3 = limit.map(l => iter2.limit(l.value.toInt)).getOrElse(iter2)
        cur  <- MongoDbIO.async(iter3.batchCursor)
      } yield cur

    def readCursor(f: AFile, offset: Natural, limit: Option[Positive])
        : Backend[BsonCursor] =
      Collection.fromFile(f).fold(
        err  => MonadFsErr[Backend].raiseError[BsonCursor](FileSystemError.pathErr(err)),
        coll => toM(cursor(coll, offset, limit)).liftB)

    def nextChunk(c: BsonCursor): Backend[(BsonCursor, Vector[Data])] =
      toM(DC.nextChunk(c).map((c, _))).liftB

    def closeCursor(c: BsonCursor): Configured[Unit] =
      toConfigured(DC.close(c))
  }

  object ManagedWriteFileModule extends ManagedWriteFileModule {
    private def dataToDocument(d: Data): FileSystemError \/ Bson.Doc =
      BsonCodec.fromData(d)
        .leftMap(err => FileSystemError.writeFailed(d, err.shows))
        .flatMap {
          case doc @ Bson.Doc(_) => doc.right
          case otherwise => FileSystemError.writeFailed(d, "MongoDB is only able to store documents").left
        }

    def writeCursor(file: AFile): Backend[Collection] =
      Collection.fromFile(file).fold(
        err => MonadFsErr[Backend].raiseError[Collection](FileSystemError.pathErr(err)),
        coll => toM(MongoDbIO.ensureCollection(coll) *> coll.point[MongoDbIO]).liftB)

    def writeChunk(c: Collection, chunk: Vector[Data])
        : Configured[Vector[FileSystemError]] = {
      val (errs, docs) = chunk foldMap { d =>
        dataToDocument(d).fold(
          e => (Vector(e), Vector()),
          d => (Vector(), Vector(d)))
      }
      val io = MongoDbIO.insertAny(c, docs.map(_.repr))
        .filter(_ < docs.size)
        .map(n => FileSystemError.partialWrite(docs.size - n))
        .run.map(errs ++ _.toList)
      toConfigured(io)
    }

    def closeCursor(c: Collection): Configured[Unit] =
      ().point[Configured]
  }

  object ManageFileModule extends ManageFileModule {
    import fs.managefile._, ManageFile._

    /** TODO: There are still some questions regarding Path
      *   1) We should assume all paths will be canonicalized and can do so
      *      with a ManageFile ~> ManageFile that canonicalizes everything.
      *
      *   2) Currently, parsing a directory like "/../foo/bar/" as an absolute
      *      dir succeeds, this should probably be changed to fail.
      */
    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      val mm: MongoManage[FileSystemError \/ Unit] =
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageInT]
      toBackend(mm)
    }

    def delete(path: APath): Backend[Unit] = {
      val mm: MongoManage[FileSystemError \/ Unit] =
        pathy.Path.refineType(path).fold(deleteDir, deleteFile)
          .run.liftM[ManageInT]
      toBackend(mm)
    }

    def tempFile(near: APath): Backend[AFile] = {
      val checkPath =
        EitherT.fromDisjunction[MongoManage](Collection.dbNameFromPath(near))
          .bimap(FileSystemError.pathErr(_), κ(()))

      val mkTemp =
        freshName.liftM[FileSystemErrT] map { n =>
          pathy.Path.refineType(near).fold(
            _ </> pathy.Path.file(n),
            f => pathy.Path.fileParent(f) </> pathy.Path.file(n))
        }

      val mm: MongoManage[FileSystemError \/ AFile] = checkPath.flatMap(κ(mkTemp)).run
      toBackend(mm)
    }
  }
}
