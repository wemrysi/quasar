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

package quasar.physical.sparkcore.fs.cassandra

import slamdata.Predef._
import quasar.contrib.pathy._

import pathy.Path._
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.apache.spark.SparkContext

sealed trait CassandraDDL[A]
final case class KeyspaceExists(keyspace: String) extends CassandraDDL[Boolean]
final case class DropKeyspace(keyspace: String) extends CassandraDDL[Unit]
final case class CreateKeyspace(keyspace: String) extends CassandraDDL[Unit]

final case class TableExists(keyspace: String, table: String) extends CassandraDDL[Boolean]
final case class DropTable(keyspace: String, table: String) extends CassandraDDL[Unit]
final case class CreateTable(keyspace: String, table: String) extends CassandraDDL[Unit]

final case class MoveTable(fromKs: String, fromTable: String, toKs: String, toTable: String) extends CassandraDDL[Unit]
final case class ListTables(keyspace: String) extends CassandraDDL[Set[String]]

object CassandraDDL {


  class Ops[S[_]](implicit s0: CassandraDDL :<: S) {
    def keyspaceExists(keyspace: String): Free[S, Boolean] = Free.liftF(s0.inj(KeyspaceExists(keyspace)))
    def tableExists(keyspace: String, table: String): Free[S, Boolean] = Free.liftF(s0.inj(TableExists(keyspace, table)))
    def dropKeyspace(keyspace: String): Free[S, Unit] = Free.liftF(s0.inj(DropKeyspace(keyspace)))
    def dropTable(keyspace: String, table: String): Free[S, Unit] = Free.liftF(s0.inj(DropTable(keyspace, table)))
    def createTable(keyspace: String, table: String): Free[S, Unit] = Free.liftF(s0.inj(CreateTable(keyspace, table)))
    def createKeyspace(keyspace: String): Free[S, Unit] = Free.liftF(s0.inj(CreateKeyspace(keyspace)))
    def moveTable(fromK: String, fromT: String, toK: String, toT: String): Free[S, Unit] = Free.liftF(s0.inj(MoveTable(fromK, fromT, toK, toT)))
    def listTables(keyspace: String): Free[S, Set[String]] = Free.liftF(s0.inj(ListTables(keyspace)))
  }

  object Ops {

    implicit def apply[S[_]](implicit s0: CassandraDDL :<: S): Ops[S] = new Ops[S]

  }

  def interpreter[S[_]](implicit sc: SparkContext) = new (CassandraDDL ~> Task) {
    def apply[A](from: CassandraDDL[A]) = 
      from match {
        case KeyspaceExists(keyspace) => 
          keyspaceExists(keyspace)
        case TableExists(keyspace, table) =>
          tableExists(keyspace, table)
        case DropKeyspace(keyspace) =>
          dropKeyspace(keyspace)
        case DropTable(keyspace, table) =>
          dropTable(keyspace, table)
        case CreateTable(keyspace, table) =>
          createTable(keyspace, table)
        case CreateKeyspace(keyspace) =>
          createKeyspace(keyspace)
        case MoveTable(fromK, fromT, toK, toT) =>
          moveTable(fromK, fromT, toK, toT)
        case ListTables(keyspace) =>
          listTables(keyspace)
      }
  }

  def keyspaceExists[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      common.keyspaceExists(keyspace)
    }
  }

  def tableExists[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      common.tableExists(keyspace, table)
    }
  }

  def dropKeyspace[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val _ = common.dropKeyspace(keyspace)
    }
  }

  def dropTable[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val _ = common.dropTable(keyspace, table)
    }
  }

  def createTable[S[_]](keyspace: String, table: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val _ = common.createTable(keyspace, table)
    }
  }

  def createKeyspace[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val _ = common.createKeyspace(keyspace)
    }
  }

  def moveTable[S[_]](fromK: String, fromT: String, toK: String, toT: String)(implicit sc: SparkContext) = Task.delay {
    val rdd = sc.cassandraTable(fromK, fromT)
    rdd.saveAsCassandraTableEx(rdd.tableDef.copy(keyspaceName = toK, tableName = toT))
  }

  def listTables[S[_]](keyspace: String)(implicit sc: SparkContext) = Task.delay {
    sc.cassandraTable[String]("system_schema", "tables")
      .select("table_name")
      .where("keyspace_name = ?", keyspace)
      .collect.toSet
  }

}

/*
 *  TODO it should be represented as algebra (e.g CassandraDDL[A]) so that it can be later used and interpreted as Free[CassandraDDL, A]
 */
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
