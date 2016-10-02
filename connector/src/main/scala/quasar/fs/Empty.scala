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

package quasar.fs

import quasar.Predef._
import scalaz._
import quasar.fp._, numeric._
import FileSystemError._
import quasar.contrib.pathy._

object Empty extends UnifiedFileSystemBuilder {
  val FsType = FileSystemType("empty")
  def apply[F[_]: Applicative] : UnifiedFileSystem[F] = new Impl[F]

  class Impl[F[_]: Applicative] extends UnifiedFileSystem[F] {
    def closeR(fh: RHandle): F[Unit]                                                     = ()
    def closeW(fh: WHandle): F[Unit]                                                     = ()
    def closeQ(rh: QHandle): F[Unit]                                                     = ()
    def createTempFile(near: APath): FLR[AFile]                                          = unknownPath(near)
    def deletePath(path: APath): FLR[Unit]                                               = unknownPath(path)
    def evaluate(lp: FixPlan): FPLR[QHandle]                                             = unknownPlan(lp)
    def execute(lp: FixPlan, out: AFile): FPLR[AFile]                                    = unknownPlan(lp)
    def exists(file: AFile): F[Boolean]                                                  = false
    def explain(lp: FixPlan): FPLR[ExecutionPlan]                                        = unknownPlan(lp)
    def list(dir: ADir): FLR[DirList]                                                    = unknownPath(dir)
    def more(rh: QHandle): FLR[Chunks]                                                   = unknownResultHandle(rh)
    def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]               = unknownPath(src)
    def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit]            = unknownPath(src)
    def openForRead(file: AFile, offset: Natural, limit: Option[Positive]): FLR[RHandle] = unknownPath(file)
    def openForWrite(file: AFile): FLR[WHandle]                                          = unknownPath(file)
    def read(fh: RHandle): FLR[Chunks]                                                   = unknownReadHandle(fh)
    def write(fh: WHandle, chunks: Chunks): F[Errors]                                    = Vector(unknownWriteHandle(fh))
  }
}
