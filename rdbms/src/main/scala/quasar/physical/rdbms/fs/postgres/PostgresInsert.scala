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

import slamdata.Predef._
import quasar.Data
import quasar.fs.FileSystemError
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.fs.RdbmsInsert
import quasar.physical.rdbms.model.{ColumnDesc, ColumnarTable, JsonTable, TableModel}

import doobie.imports._
import scalaz._
import Scalaz._

trait PostgresInsert extends RdbmsInsert {

  implicit def dataMeta: Meta[Data]

  def toColValues(cols: Set[ColumnDesc])(row: Data): \/[FileSystemError, List[String]] = {
    row match {
      case Data.Obj(fields) =>
        (fields.toList.filter(f => cols.exists(_.name === f._1)).sortBy(_._1).map {
          case (_, v) =>
            v match {
              case Data.Obj(innerJs) => "{TODO decode this to JS string}"// TODO
              case Data.Int(num) => s"$num"
              case Data.Str(txt) => s"'$txt'"
              case _ => s"TODO" // TODO
            }
        }).right
      case _ => FileSystemError.unsupportedOperation(s"Unexpected data row $row not matching model $cols").left
    }
  }

  def batchInsert(
      dbPath: TablePath,
      chunk: Vector[Data],
      model: TableModel
  ): ConnectionIO[Vector[FileSystemError]] = {

    val insertIntoTable = fr"insert into " ++ Fragment.const(dbPath.shows)

    model match {
      case JsonTable =>
        val fQuery = fr"(data) values(?::JSON)"
        Update[Data](fQuery.update.sql)
          .updateMany(chunk)
          .map(_ => Vector.empty[FileSystemError])
      case ColumnarTable(cols) =>
        (chunk.traverse(toColValues(cols)).traverse {
            valRows => valRows.traverse { valRow =>
              val query = insertIntoTable ++ fr"(" ++ Fragment.const(cols.map(_.name).mkString(",")) ++
                fr") values (" ++
                valRow.map(v => Fragment.const(v)).intercalate(fr",") ++ fr")"
              query.update.run.void
            }
        }).map {
          case -\/(err) => Vector(err)
          case _ => Vector.empty
        }
    }
  }
}
