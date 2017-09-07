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

import java.util.UUID

import doobie.free.connection.ConnectionIO
import doobie.imports.Update
import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.eitherT._
import quasar.{Data, DataCodec}
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fs._
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.mapping.json._
import quasar.physical.rdbms.Rdbms
import slamdata.Predef._
import doobie.syntax.string._
import doobie.util.fragment.Fragment
import quasar.effect._
import quasar.fp.free._
import slamdata.Predef._
import quasar.physical.rdbms.mapping._

import scalaz._
import Scalaz._

trait RdbmsWriteFile {
  this: Rdbms =>

  import WriteFile._

  val writeKvs = KeyValueStore.Ops[WriteHandle, TablePath, Eff]

  implicit private val monadMInstance: Monad[M] = MonadM

  override def WriteFileModule: WriteFileModule = new WriteFileModule {

    def batchInsert(
        dbPath: TablePath,
        chunk: Vector[Data],
        isJson: Boolean
        ): ConnectionIO[Vector[FileSystemError]] = {

      // TODO this is postgres-specific (?::JSON)
      val fQuery = fr"insert into " ++ Fragment.const(dbPath.shows) ++ fr"(id, data) values(?, ?::JSON)"

          Update[Data](fQuery.update.sql)(JsonDataComposite)
            .updateMany(chunk.toList)
            .map(_ => Vector.empty[FileSystemError])
      }

    private def getDbPath(h: WriteHandle) = ME.unattempt(
      writeKvs
        .get(h)
        .toRight(FileSystemError.unknownWriteHandle(h))
        .run
        .liftB)

    override def write(
        h: WriteHandle,
        chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      (for {
        _ <- ME.unattempt(writeKvs.get(h).toRight(FileSystemError.unknownWriteHandle(h)).run.liftB)
        dbPath <- getDbPath(h)
       isJson <- ME.unattempt(lift(describeTable.isJson(dbPath).run).into[Eff].liftB)
        _ <- lift(batchInsert(dbPath, chunk, isJson)).into[Eff].liftB
      } yield Vector()).run.value.map(_.valueOr(Vector(_)))
    }

    override def open(file: AFile): Backend[WriteHandle] =
      for {
        i <- MonotonicSeq.Ops[Eff].next.liftB
        dbPath = TablePath.create(file)
        _ <- ME.unattempt(lift(createTable.run(dbPath).run).into[Eff].liftB)
        handle = WriteHandle(file, i)
        _ <- writeKvs.put(handle, dbPath).liftB
      } yield handle

    override def close(h: WriteHandle): Configured[Unit] =
      writeKvs.delete(h).liftM[ConfiguredT]

  }
}
