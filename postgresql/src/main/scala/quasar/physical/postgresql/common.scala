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

package quasar.physical.postgresql

import quasar.Predef._
import quasar.fs._

import doobie.imports._
import pathy.Path, Path._
import scalaz._, Scalaz._
import shapeless.HNil

object common {

  final case class DbTable(db: String, table: String)

  // TODO: Going with ☠ style path table names for the moment
  def dbTableFromPath(f: APath): FileSystemError \/ DbTable =
    Path.flatten(None, None, None, Some(_), Some(_), f)
      .toIList.unite.uncons(
        FileSystemError.pathErr(PathError.invalidPath(f, "no database specified")).left,
        (h, t) => DbTable(h, t.intercalate("☠")).right)

  def pathSegmentsFromPrefix(
    tableNamePathPrefix: String, tableNamePaths: List[String]
  ): Set[PathSegment] =
    tableNamePaths
      .map(_.stripPrefix(tableNamePathPrefix).stripPrefix("☠").split("☠").toList)
      .collect {
        case h :: Nil => FileName(h).right
        case h :: _   => DirName(h).left
      }.toSet

  def tableExists[S[_]](tableName: String): ConnectionIO[Boolean] = {
    val qStr =
      s"""select table_name from information_schema.tables
          where table_name = '$tableName'"""

     Query[HNil, String](qStr, none).toQuery0(HNil).list.map(_.nonEmpty)
  }

  def tablesWithPrefix(tableNamePrefix: String): ConnectionIO[List[String]] = {
    val qStr =
      s"""select table_name from information_schema.tables
          where table_name like '$tableNamePrefix%'"""

    Query[HNil, String](qStr, none).toQuery0(HNil).list
  }

}
