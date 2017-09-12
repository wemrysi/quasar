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

package quasar.physical.rdbms.fs

import doobie.imports._
import pathy.Path
import pathy.Path._
import quasar.contrib.pathy.{ADir, AFile, APath}
import quasar.contrib.scalaz.eitherT._
import quasar.effect.MonotonicSeq
import quasar.fp.free.lift
import quasar.fs._
import quasar.physical.rdbms.Rdbms
import quasar.physical.rdbms.common.{SchemaName, TablePath}
import slamdata.Predef._
import scalaz.Scalaz._
import scalaz._

trait RdbmsManageFile extends RdbmsDescribeTable with RdbmsCreateTable {
  this: Rdbms =>
  implicit def MonadM: Monad[M]

  def dropSchema(schemaName: SchemaName): ConnectionIO[Unit] =
    (fr"DROP SCHEMA" ++ Fragment.const(schemaName.name) ++ fr"CASCADE").update.run
      .map(_ => ())

  def dropSchemaWithChildren(parent: SchemaName): Backend[Unit] =
    lift(findChildSchemas(parent).flatMap(cs => (cs :+ parent).foldMap(dropSchema)))
      .into[Eff]
      .liftB

  override def ManageFileModule = new ManageFileModule {

    override def move(
        scenario: ManageFile.MoveScenario,
        semantics: MoveSemantics): Backend[Unit] =
      // TODO
      ().point[Backend]

    def deleteTable(aFile: AFile): Backend[Unit] = {
      val dbTablePath = TablePath.create(aFile)
      (for {
        exists <- lift(tableExists(dbTablePath)).into[Eff].liftB
        _ <- exists.unlessM(
          ME.raiseError(FileSystemError.pathErr(PathError.pathNotFound(aFile))))
        _ <- lift(
          (fr"DROP TABLE" ++ Fragment.const(dbTablePath.shows)).update.run)
          .into[Eff]
          .liftB
      } yield ())
    }

    def deleteSchema(aDir: ADir): Backend[Unit] = {
      TablePath
        .dirToSchemaName(aDir)
        .map { schemaName =>
          (for {
            exists <- lift(schemaExists(schemaName)).into[Eff].liftB
            _ <- exists.unlessM(ME.raiseError(
              FileSystemError.pathErr(PathError.pathNotFound(aDir))))
            _ <- dropSchemaWithChildren(schemaName)
          } yield ())
        }
        .getOrElse(().point[Backend])
    }

    override def delete(path: APath): Backend[Unit] = {
      Path
        .maybeFile(path)
        .map(deleteTable)
        .orElse(Path.maybeDir(path).map(deleteSchema))
        .getOrElse(().point[Backend])
    }

    override def tempFile(near: APath): Backend[AFile] = {
      def tempFilePath(near: APath): Backend[AFile] = {
        MonotonicSeq
          .Ops[Eff]
          .next
          .map { i =>
            val tmpFilename = file(s"__quasar_tmp_table_$i")
            refineType(near).fold(d => {
              d </> tmpFilename
            }, f => fileParent(f) </> tmpFilename)
          }
          .liftB
      }

      for {
        path <- tempFilePath(near)
        _ <- lift(createTable(TablePath.create(path))).into[Eff].liftB
      }
        yield path
    }

  }
}
