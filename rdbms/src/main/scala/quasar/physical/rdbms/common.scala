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

package quasar.physical.rdbms

import slamdata.Predef._
import pathy.Path
import pathy.Path.PathCodec
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo

import scalaz._, Scalaz._

object common {

  final case class Config(connInfo: JdbcConnectionInfo)

  final case class SchemaName(name: String) extends AnyVal
  final case class TableName(name: String) extends AnyVal
  final case class TablePath(schema: Option[SchemaName], table: TableName)


  implicit val showPath: Show[TablePath] =
    Show.shows(path => path.schema.map(s => s"${s.name}.").getOrElse("") + path.table.name)

  object TablePath {

    val Separator = "_$child_"

    def dirToSchemaName(dir: ADir): Option[SchemaName] = {
      Some(Path.flatten(None, None, None, Some(_), Some(_), dir)
        .toIList
        .unite
        .intercalate(Separator))
        .filter(_.nonEmpty).map(SchemaName.apply)
    }


    def create(file: AFile): TablePath = {
      val filename = Path.fileName(file).value
      val parentDir = Path.parentDir(file)
      val dirname = parentDir.flatMap(dirToSchemaName)
      new TablePath(dirname, TableName(filename))
    }
  }
}
