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

import quasar.Data
import quasar.physical.rdbms.fs.RdbmsCreateTable
import quasar.fs.FileSystemErrT
import quasar.physical.rdbms.common._
import slamdata.Predef._
import quasar.fs._
import doobie.imports._
import scalaz.EitherT
import scalaz.syntax.show._

class PostgresCreateJsonTable extends RdbmsCreateTable {

  override def run(
      tablePath: TablePath,
      firstRow: Data): FileSystemErrT[ConnectionIO, Unit] =
    EitherT(sql"CREATE TABLE IF NOT EXISTS ${tablePath.shows} (ID serial NOT NULL PRIMARY KEY, data json NOT NULL)".update.run
      .map(_ => ())
      .attemptSomeSqlState {
        case errorCode =>
          FileSystemError.writeFailed(
            firstRow,
            s"Failed to create table ${tablePath.shows}. SQL error $errorCode")
      })

}
