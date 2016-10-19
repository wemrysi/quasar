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

package quasar.physical.jsonfile.fs

import quasar._
import quasar.Predef._
import quasar.fs._, PathError._
import quasar.fs.FileSystemError._
import quasar.effect._
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._
import Scalaz.{ ToIdOps => _, _ }
import FileSystemIndependentTypes._
import MoveSemantics._
// import ygg.table._

class FsAlgebras[S[_]] extends STypes[S] {
  private def defaultChunkSize: Int = 10
  def emptyData(): Chunks = Vector()

  def ignoreRes[A](x: FS[A]): FS[LR[Unit]] = x map (_ => ().right)

  def existsError[A](path: APath): FLR[A]         = point(-\/(pathErr(pathExists(path))))
  def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))
  def nextLong(implicit MS: MonotonicSeq :<: S)   = MonotonicSeq.Ops[S].next

  def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]    = ()
  def deleteDir(x: ADir)(implicit KVF: KVFile[S]): FLR[Unit]                = ()
  def deleteFile(x: AFile)(implicit KVF: KVFile[S]): FLR[Unit]              = KVF delete x map (_ fold (().right, unknownPath(x).left))

  def newWHandle(file: AFile)(implicit MS: MonotonicSeq :<: S): FS[WHandle] = nextLong map (uid => WHandle(file, uid))

  def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics)(implicit KVF: KVFile[S]): FLR[Unit] = semantics match {
    case Overwrite     => KVF.move(src, dst) map (_ fold (().right, unknownPath(src).left))
    case FailIfExists  => KVF contains dst flatMap (_ fold (existsError(dst), ignoreRes(KVF.move(src, dst))))
    case FailIfMissing => KVF contains dst flatMap (_ fold (ignoreRes(KVF.move(src, dst)), unknownPath(dst)))
  }
  def openForRead(file: AFile, offset: Int, limit: Int)(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]): FLR[RHandle] = {
    KVF get file flatMap {
      case Some(data) => nextLong flatMap (uid => RHandle(file, uid) |> (h => KVR.put(h, ReadPos(data, offset, limit)) map (_ => h.right)))
      case _          => point(unknownPath(file).left)
    }
  }
  def createFile(file: AFile, uid: Long)(implicit KVF: KVFile[S], KVW: KVWrite[S]): FLR[WHandle] = {
    val wh = WHandle(file, uid)
    for {
      _ <- KVF.put(file, emptyData())
      _ <- KVW.put(wh, emptyData())
    } yield wh
  }
  def createTempFile(near: APath, uid: Long)(implicit KVF: KVFile[S]): FS[LR[AFile]] = {
    val name  = file(tmpName(uid))
    val rfile = refineType(near).fold(_ </> name, f => fileParent(f) </> name)

    KVF.put(rfile, emptyData()) map (_ => rfile)
  }

  def read(fh: RHandle)(implicit KVF: KVFile[S], KVR: KVRead[S]): FLR[Chunks] = KVR get fh map {
    case Some(ReadPos(data, offset, limit)) => (data drop offset take limit).right
    case _                                  => unknownReadHandle(fh).left
  }
  def write(fh: WHandle, chunk: Chunks)(implicit KVW: KVWrite[S]): FS[Errors] = KVW get fh flatMap {
    case Some(data) => KVW.put(fh, data ++ chunk) map (_ => Vector())
    case _          => Vector(unknownWriteHandle(fh))
  }

  def manageFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]) = λ[ManageFile ~> FS] {
    case ManageFile.Move(scenario, semantics) => scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
    case ManageFile.Delete(path)              => refineType(path).fold(deleteDir, deleteFile)
    case ManageFile.TempFile(path)            => nextLong flatMap (uid => createTempFile(path, uid))
  }
  def writeFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]) = λ[WriteFile ~> FS] {
    case WriteFile.Open(file)        => nextLong flatMap (uid => createFile(file, uid))
    case WriteFile.Write(fh, chunks) => write(fh, chunks)
    case WriteFile.Close(fh)         => for (_ <- KVW delete fh) yield ()
  }
  def readFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]) = λ[ReadFile ~> FS] {
    case ReadFile.Open(file, offset, None)        => openForRead(file, offset.get.toInt, defaultChunkSize)
    case ReadFile.Open(file, offset, Some(limit)) => openForRead(file, offset.get.toInt, limit.get.toInt)
    case ReadFile.Read(fh)                        => read(fh)
    case ReadFile.Close(fh)                       => for (_ <- KVR delete fh) yield ()
  }
  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) = λ[QueryFile ~> FS] {
    case QueryFile.ExecutePlan(lp, out) => phaseResults(lp) tuple \/-(out)
    case QueryFile.EvaluatePlan(lp)     => (phaseResults(lp) |@| nextLong)((ph, uid) => ph -> \/-(QHandle(uid)))
    case QueryFile.Explain(lp)          => phaseResults(lp) tuple ExecutionPlan(FsType, "...")
    case QueryFile.More(rh)             => Vector()
    case QueryFile.ListContents(dir)    => KVF.keys map (_ flatMap pathName) map (xs => makeDirList(xs: _*).right)
    case QueryFile.Close(fh)            => for (_ <- KVQ delete fh) yield ()
    case QueryFile.FileExists(file)     => KVF contains file
  }
}
