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
import quasar.fs._
import quasar.fs.FileSystemError._
import quasar.effect._
import pathy.Path._
import quasar.contrib.pathy._
import scalaz._
import Scalaz.{ ToIdOps => _, _ }
import FileSystemIndependentTypes._

class FsAlgebras[S[_]] extends STypes[S] {
  def phaseResults(lp: FixPlan): FS[PhaseResults] = Vector(PhaseResult.Detail("jsonfile", "<no description>"))
  def nextLong(implicit MS: MonotonicSeq :<: S)   = MonotonicSeq.Ops[S].next

  def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]    = ()
  def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit] = ()

  def createTempFile(near: APath, uid: Long): LR[AFile] = {
    val name = file(tmpName(uid))
    refineType(near).fold(
      _ </> name,
      f => fileParent(f) </> name
    ).right
  }

  def manageFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S]) = λ[ManageFile ~> FS] {
    case ManageFile.Move(scenario, semantics) => scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
    case ManageFile.Delete(path)              => refineType(path).fold(_ => Unimplemented, KVF delete _ map (_ => ().right))
    case ManageFile.TempFile(path)            => nextLong map (uid => createTempFile(path, uid))
  }
  def writeFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVW: KVWrite[S]) = λ[WriteFile ~> FS] {
    case WriteFile.Open(file)        => nextLong flatMap (uid => create[S](file, uid) map (wh => wh.right))
    case WriteFile.Write(fh, chunks) => KVW get fh fold (wv => write(wv, chunks), Vector(unknownWriteHandle(fh)))
    case WriteFile.Close(fh)         => KVW delete fh
  }
  def readFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVR: KVRead[S]) = λ[ReadFile ~> FS] {
    case ReadFile.Open(file, offset, limit) => nextLong map (uid => RHandle(file, uid).right)
    case ReadFile.Read(fh)                  => KVR get fh fold (rv => read(rv), unknownReadHandle(fh).left)
    case ReadFile.Close(fh)                 => KVR delete fh
  }
  def queryFile(implicit MS: MonotonicSeq :<: S, KVF: KVFile[S], KVQ: KVQuery[S]) = λ[QueryFile ~> FS] {
    case QueryFile.ExecutePlan(lp, out) => phaseResults(lp) tuple \/-(out)
    case QueryFile.EvaluatePlan(lp)     => (phaseResults(lp) |@| nextLong)((ph, uid) => ph -> \/-(QHandle(uid)))
    case QueryFile.Explain(lp)          => phaseResults(lp) tuple ExecutionPlan(FsType, "...")
    case QueryFile.More(rh)             => Vector()
    case QueryFile.ListContents(dir)    => KVF.keys map (_ flatMap pathName) map (xs => makeDirList(xs: _*).right)
    case QueryFile.Close(fh)            => KVQ delete fh
    case QueryFile.FileExists(file)     => KVF contains file
  }
}
