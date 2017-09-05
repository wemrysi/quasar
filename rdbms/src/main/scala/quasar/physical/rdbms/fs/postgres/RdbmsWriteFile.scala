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

package quasar.physical.rdbms.fs.postgres

import doobie.imports.Update
import doobie.syntax.connectionio._
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs._
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.Rdbms
import slamdata.Predef._
import doobie.util.transactor.Transactor
import doobie.syntax.string._
import quasar.effect._
import quasar.fp.free._
import slamdata.Predef._
import quasar.physical.rdbms.mapping._

import scalaz._
import Scalaz._
import scalaz.concurrent.Task

trait RdbmsWriteFile {
  this: Rdbms =>

  import WriteFile._

  val writeKvs = KeyValueStore.Ops[WriteHandle, TablePath, Eff]

  implicit private val monadMInstance: Monad[M] = MonadM

  override def WriteFileModule: WriteFileModule = new WriteFileModule {

    def batchInsert(
        dbPath: TablePath,
        chunk: Vector[Data],
        xa: Transactor[Task]
        ): Task[Vector[FileSystemError]] = {
      chunk.headOption match {
        case Some(Data.Obj(lm)) =>
          val fQuery = fr"insert into ${dbPath.shows}" ++ fr"values(" ++ lm.toList
            .map(_ => fr"""?""")
            .intercalate(fr",") ++ fr")"

          Update[Data](fQuery.update.sql)
            .updateMany(chunk.toList)
            .transact(xa) map (_ => Vector())
        case Some(unsupportedObj) =>
          Task.delay(
            Vector(
              FileSystemError.writeFailed(
                (unsupportedObj, "Cannot translate to a RDBMS row."))))
        case None =>
          Task.delay(Vector())
      }
    }

    override def write(
        h: WriteHandle,
        chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      (for {
        xa <- MR.asks(_.transactor)
        dbPath <- ME.unattempt(
          writeKvs
            .get(h)
            .toRight(FileSystemError.unknownWriteHandle(h))
            .run
            .liftB)
        _ <- lift(batchInsert(dbPath, chunk, xa)).into[Eff].liftB
      } yield Vector()).run.value.map(_.valueOr(Vector(_)))
    }

    override def open(file: AFile): Backend[WriteHandle] =
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        dbPath = TablePath.create(file)
        handle = WriteHandle(file, i)
        _ <- writeKvs.put(handle, dbPath).liftB
      } yield handle

    override def close(h: WriteHandle): Configured[Unit] =
      writeKvs.delete(h).liftM[ConfiguredT]

  }
}
