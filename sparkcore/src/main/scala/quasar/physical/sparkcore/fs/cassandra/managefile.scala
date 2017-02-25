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

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.fs._,
  ManageFile._,
  MoveScenario._,
  FileSystemError._,
  PathError._
import quasar.effect._
import quasar.fp.free._
import quasar.fs.impl.ensureMoveSemantics

import org.apache.spark.SparkContext
import com.datastax.driver.core.Session
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import pathy._, Path._
import scalaz._, Scalaz._

object managefile {

  import common._

  def chrooted[S[_]](prefix: ADir)(implicit
    s0: Read.Ops[SparkContext, S]
  ): ManageFile ~> Free[S, ?] =
    flatMapSNT(interpret) compose chroot.manageFile[ManageFile](prefix)
  
  def interpret[S[_]](implicit
    s0: Read.Ops[SparkContext, S]
    ): ManageFile ~> Free[S, ?] = 
    new (ManageFile ~> Free[S, ?]) {
      def apply[A](mf: ManageFile[A]): Free[S, A] = mf match {
        case Move(FileToFile(sf, df), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sf, df, pathExists _, semantics).toLeft(()) *>
            moveFile(sf, df).liftM[FileSystemErrT]).run
        case Move(DirToDir(sd, dd), semantics) =>
          (ensureMoveSemantics[Free[S, ?]](sd, dd, pathExists _, semantics).toLeft(()) *>
            moveDir(sd, dd).liftM[FileSystemErrT]).run
        case Delete(path) => delete(path)
        case TempFile(near) => tempFile(near)
      }
    }

  def pathExists[S[_]](path: APath)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Boolean] = 
    read.asks { sc =>
      CassandraConnector(sc.getConf).withSessionDo { implicit session =>
        refineType(path).fold(
          d => keyspaceExists(keyspace(d)),
          f => tableExists(keyspace(fileParent(f)), tableName(f))
        )
    }
  }

  def moveFile[S[_]](sf: AFile, df: AFile)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = {
    read.asks { implicit sc =>
      moveTable(sf, df)
    }
  }

  private def moveTable(sf: AFile, df: AFile)(implicit sc: SparkContext) = {
    val dks = keyspace(fileParent(df))
    val dft = tableName(df)
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      val u1 = if(!keyspaceExists(dks)){
        createKeyspace(dks)
      }
      val u2 = if(tableExists(dks, dft)) {
        dropTable(dks, dft)
      }
      val rdd = sc.cassandraTable(keyspace(fileParent(sf)), tableName(sf))
      rdd.saveAsCassandraTableEx(rdd.tableDef.copy(keyspaceName = keyspace(fileParent(df)), tableName = tableName(df)))

      val r = dropTable(keyspace(fileParent(sf)), tableName(sf))
    }
  }

  def moveDir[S[_]](sd: ADir, dd: ADir)(implicit 
    read: Read.Ops[SparkContext, S]
    ): Free[S, Unit] = {
    read.asks { implicit sc =>
      val srcTables = sc.cassandraTable[String]("system_schema", "tables")
        .select("table_name")
        .where("keyspace_name = ?", keyspace(sd))
        .collect.toSet

      srcTables.foreach { tn =>
        moveTable(sd </> file(tn), dd </> file(tn))
      }

      val _ = CassandraConnector(sc.getConf).withSessionDo { implicit session =>
        dropKeyspace(keyspace(sd))
      }
    }
  }

  def delete[S[_]](path: APath)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ Unit] = 
  read.asks { sc =>
    CassandraConnector(sc.getConf).withSessionDo { implicit session =>
      maybeFile(path).fold(deleteDir(path, sc))(file => deleteFile(file))
    }
  }

  def deleteFile(file: AFile)(implicit session: Session) = 
    if(keyspaceExists(keyspace(fileParent(file))) && tableExists(keyspace(fileParent(file)), tableName(file))) {
      val r = dropTable(keyspace(fileParent(file)), tableName(file))
      ().right[FileSystemError]
    } else {
      pathErr(pathNotFound(file)).left[Unit]
    }

  def deleteDir(dir: APath, sc: SparkContext)(implicit session: Session) = {  
    val aDir: ADir = refineType(dir).fold(d => d, fileParent(_))
    val ks = keyspace(aDir)
    val dirs = sc.cassandraTable[String]("system_schema","keyspaces").select("keyspace_name")
      .filter(_.startsWith(ks))
      .collect.toSet

    if(!dirs.isEmpty){
      dirs.foreach( d => dropKeyspace(d))
      ().right[FileSystemError]
    } else {
      pathErr(pathNotFound(dir)).left[Unit]
    }
  }

  def tempFile[S[_]](near: APath)(implicit
    read: Read.Ops[SparkContext, S]
    ): Free[S, FileSystemError \/ AFile] = 
    read.asks { sc =>
      val randomFileName = s"q${scala.math.abs(scala.util.Random.nextInt(9999))}"
      val aDir: ADir = refineType(near).fold(d => d, fileParent(_))
      (aDir </> file(randomFileName)).right[FileSystemError]
    }

}
