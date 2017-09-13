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
import pathy.Path.DirName
import quasar.contrib.pathy.{ADir, AFile}
import quasar.physical.rdbms.jdbc.JdbcConnectionInfo

import scalaz._
import Scalaz._

object common {

  final case class Config(connInfo: JdbcConnectionInfo)

  sealed trait Schema

  final case object DefaultSchema extends Schema

  final case class CustomSchema(name: String) extends Schema


  object Schema {
    def lastDirName(schema: Schema): DirName = {
      schema match {
        case DefaultSchema => DirName("")
        case CustomSchema(name) =>
          val lastSeparatorIndex = name.lastIndexOf(TablePath.Separator)
          val startIndex = if (lastSeparatorIndex > 0) lastSeparatorIndex + TablePath.Separator.length else 0
          DirName(name.substring(startIndex))
      }
    }
  }

  final case class TableName(name: String) extends AnyVal
  final case class TablePath(schema: Schema, table: TableName)

  implicit val showSchema: Show[Schema] = Show.shows {
    case DefaultSchema => ""
    case CustomSchema(name) => s"$name"
  }

  implicit val showTableName: Show[TableName] =
    Show.shows(_.name)

  implicit val showPath: Show[TablePath] = Show.shows { tp =>
    tp.schema match {
      case DefaultSchema => tp.table.shows
      case CustomSchema(name) => s"$name.${tp.table.shows}"
    }
  }

  object TablePath {

    val Separator = "__child_"
    val SeparatorRegex = "__child_"

    def dirToSchema(dir: ADir): Schema = {
      Path.flatten(None, None, None, Some(_), Some(_), dir)
        .toIList
        .unite
        .intercalate(Separator) match {
        case "" => DefaultSchema
        case pathStr => CustomSchema(pathStr)
      }

    }

    def create(file: AFile): TablePath = {
      val filename = Path.fileName(file).value
      val schema = Path.parentDir(file).map(dirToSchema).getOrElse(DefaultSchema)
      new TablePath(schema, TableName(filename))
    }
  }
}
