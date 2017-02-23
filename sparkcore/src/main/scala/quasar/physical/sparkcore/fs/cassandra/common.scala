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

package quasar.physical.sparkcore.fs.cassandra

import quasar.Predef._
import quasar.contrib.pathy._

import pathy.Path._
import com.datastax.driver.core.Session

object common {

  def keyspace(dir: ADir) =
    posixCodec.printPath(dir).substring(1).replace("/", "_").toLowerCase

  def tableName(file: AFile) =
    posixCodec.printPath(file).split("/").reverse(0).toLowerCase

  def keyspaceExists(keyspace: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?;")
    session.execute(stmt.bind(keyspace)).all().size() > 0
  }

  def tableExists(keyspace: String, table: String)(implicit session: Session) = {
    val stmt = session.prepare("SELECT * FROM system_schema.tables WHERE keyspace_name = ? AND table_name = ?;")
    session.execute(stmt.bind(keyspace, table)).all().size() > 0
  }

  def dropKeyspace(keyspace: String)(implicit session: Session) = {
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace;")
  }

  def dropTable(keyspace: String, table: String)(implicit session: Session) = {
    session.execute(s"DROP TABLE $keyspace.$table;")
  }

  def createKeyspace(keyspace: String)(implicit session: Session) =
    session.execute(s"CREATE KEYSPACE $keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}")

  def createTable(keyspace: String, table: String)(implicit session: Session) = {
    session.execute(s"CREATE TABLE $keyspace.$table (id timeuuid PRIMARY KEY, data text);")
  }

  def insertData(keyspace: String, table: String, data: String)(implicit session: Session) = {
    val stmt = session.prepare(s"INSERT INTO $keyspace.$table (id, data) VALUES (now(),  ?);")
    session.execute(stmt.bind(data))
  }

}
