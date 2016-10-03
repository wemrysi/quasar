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

package quasar.physical.jsonfile

import quasar.Predef._
import quasar.fp.numeric._
import quasar.fs._
import FileSystemError.Unimplemented
import quasar.contrib.pathy._
import matryoshka.Fix
import quasar.sql._
import scalaz._

package object fs extends UnifiedFileSystemBuilder {
  val FsType = FileSystemType("jsonfile")

  def apply[F[_]: Applicative] = new Impl[F]

  def fixSql(query: String): Fix[Sql]  = fixQuery(Query(query))
  def fixQuery(query: Query): Fix[Sql] = fixParser parse query valueOr (_ => Fix(nullLiteral()))
}

package fs {
  class Impl[F[_]: Applicative] extends UnifiedFileSystem[F] {
    def closeR(fh: RHandle): F[Unit]                                                     = ()
    def closeW(fh: WHandle): F[Unit]                                                     = ()
    def closeQ(rh: QHandle): F[Unit]                                                     = ()
    def createTempFile(near: APath): FLR[AFile]                                          = Unimplemented
    def deletePath(path: APath): FLR[Unit]                                               = Unimplemented
    def evaluate(lp: FixPlan): FPLR[QHandle]                                             = Unimplemented
    def execute(lp: FixPlan, out: AFile): FPLR[AFile]                                    = Unimplemented
    def exists(file: AFile): F[Boolean]                                                  = false
    def explain(lp: FixPlan): FPLR[ExecutionPlan]                                        = Unimplemented
    def list(dir: ADir): FLR[DirList]                                                    = Unimplemented
    def more(rh: QHandle): FLR[Chunks]                                                   = Unimplemented
    def moveDir(src: ADir, dst: ADir, semantics: MoveSemantics): FLR[Unit]               = Unimplemented
    def moveFile(src: AFile, dst: AFile, semantics: MoveSemantics): FLR[Unit]            = Unimplemented
    def openForRead(file: AFile, offset: Natural, limit: Option[Positive]): FLR[RHandle] = Unimplemented
    def openForWrite(file: AFile): FLR[WHandle]                                          = Unimplemented
    def read(fh: RHandle): FLR[Chunks]                                                   = Unimplemented
    def write(fh: WHandle, chunks: Chunks): F[Errors]                                    = Vector(Unimplemented)
  }
}
