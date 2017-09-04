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


import quasar.contrib.pathy.AFile
import quasar.contrib.scalaz.eitherT._
import quasar.Data
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.free.lift
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.mapping._
import quasar.physical.rdbms.Rdbms
import slamdata.Predef._
import eu.timepit.refined.api.RefType.ops._
import doobie.util.transactor.Transactor
import doobie.syntax.connectionio._
import doobie.syntax.process._
import doobie.syntax.string._

import scalaz._
import Scalaz._
import scalaz.concurrent.Task

final case class SqlReadCursor(data: Vector[Data])

trait RdbmsReadFile {
  this: Rdbms =>

  import ReadFile._

  private val kvs = KeyValueStore.Ops[ReadHandle, SqlReadCursor, Eff]

  implicit def monadMInstance: Monad[M]

  override def ReadFileModule: ReadFileModule = new ReadFileModule {

    private def readAll[F[_]: Monad](
        dbPath: TablePath,
        offset: Int,
        limit: Option[Int],
        xa: Transactor[F]
    ): F[Vector[Data]] = {

      val streamWithOffset = sql"select * from ${dbPath.shows}"
        .query[Data]
        .process
        .drop(offset)

      val streamWithLimit = limit match {
        case Some(l) => streamWithOffset.take(l)
        case None    => streamWithOffset
      }
      streamWithLimit.vector.transact(xa)
    }

    private def toInt(long: Long, varName: String): Backend[\/[FileSystemError, Int]] =
      lift(
        Task.delay(
          \/.fromTryCatchNonFatal(long.toInt)
            .leftMap(
              _ => FileSystemError.readFailed(long.toString, s"$varName not convertible to Int.")
            )
        )
      ).into[Eff].liftB

    override def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] =
      for {
        xa        <- MR.asks(_.transactor)
        offsetInt <- includeError(toInt(offset.unwrap, "offset"))
        limitInt  <- limit.traverse(l => includeError(toInt(l.unwrap, "limit")))
        i         <- MonotonicSeq.Ops[Eff].next.liftB
        dbPath = TablePath.create(file)
        sqlResult <- lift(readAll(dbPath, offsetInt, limitInt, xa)).into[Eff].liftB
        handle = ReadHandle(file, i)
        _ <- kvs.put(handle, SqlReadCursor(sqlResult)).liftB

      } yield handle

    override def read(h: ReadHandle): Backend[Vector[Data]] = {
      for {
        c <- ME.unattempt(kvs.get(h).toRight(FileSystemError.unknownReadHandle(h)).run.liftB)
        data = c.data
        _ <- kvs.put(h, SqlReadCursor(Vector.empty)).liftB
      } yield data
    }

    override def close(h: ReadHandle): Configured[Unit] =
      kvs.delete(h).liftM[ConfiguredT]

  }
}
