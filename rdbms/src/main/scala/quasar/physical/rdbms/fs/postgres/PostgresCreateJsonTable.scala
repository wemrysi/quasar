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

import quasar.physical.rdbms.fs.RdbmsCreateTable
import quasar.physical.rdbms.common._
import slamdata.Predef._

import doobie.syntax.string._
import doobie.free.connection.ConnectionIO
import doobie.util.fragment.Fragment
import scalaz.syntax.monad._
import scalaz.syntax.show._

trait PostgresCreateJsonTable extends RdbmsCreateTable {

  override def createTable(tablePath: TablePath): ConnectionIO[Unit] = {

    val createSchema: ConnectionIO[Unit] = tablePath.schema.map { s =>
      (fr"CREATE SCHEMA IF NOT EXISTS" ++ Fragment.const(s.name)).update.run.map(_ => ())
    }.getOrElse(().point[ConnectionIO])

    createSchema
      .flatMap(_ =>
        (fr"CREATE TABLE IF NOT EXISTS"
          ++ Fragment.const(tablePath.shows)
          ++ fr"(data json NOT NULL)").update.run)
      .map(_ => ())
  }
}
